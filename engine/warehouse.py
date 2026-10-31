"""Warehouse access used by gold.py, runlog.py and the dashboard.

Two backends, chosen by env WAREHOUSE:
  postgres  (local)  SQLAlchemy + psycopg2, env PG_HOST / PG_PORT / PG_USER / PG_PASSWORD / PG_DB
  redshift  (aws)    boto3 'redshift-data' API against Redshift Serverless, env REDSHIFT_WORKGROUP / REDSHIFT_DB /
                     REDSHIFT_SECRET_ARN / AWS_REGION. No driver, no password in code: the Data API reads the
                     credentials from Secrets Manager (SecretArn) using the job's IAM role.

Same small interface for both:
  execute(sql)            one statement (committed)
  execute_script(text)    a .sql file: statements end with ';' at end of line, '--' comment lines skipped;
                          all statements run in ONE transaction
  query(sql)              -> pandas DataFrame (numeric -> float, date -> datetime.date, timestamp -> Timestamp)
  insert_df(table, df)    multi-row INSERT ... VALUES, chunks < 90 KB (Data API limit is 100 KB per statement)
  table_exists(name), columns(name)
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
import time
from decimal import Decimal
from functools import lru_cache

import pandas as pd

MAX_STATEMENT_BYTES = 90_000


# ----------------------------------------------------------------------------------------------- SQL text helpers
def split_sql(sql_text: str) -> list[str]:
    """Split a script into statements: a statement ends with ';' at the end of a line. '--' comment lines skipped."""
    statements, buf = [], []
    for line in sql_text.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            st = "\n".join(buf).strip().rstrip(";").strip()
            if st:
                statements.append(st)
            buf = []
    rest = "\n".join(buf).strip()
    if rest:
        statements.append(rest)
    return statements


def _is_missing(v) -> bool:
    if v is None or v is pd.NA or v is pd.NaT:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    if isinstance(v, Decimal) and (v.is_nan() or v.is_infinite()):
        return True
    try:
        import numpy as np

        if isinstance(v, np.floating) and np.isnan(v):
            return True
        if isinstance(v, np.datetime64) and np.isnat(v):
            return True
    except ImportError:  # pragma: no cover
        pass
    return False


class Warehouse:
    """Base class: SQL literal rendering + insert_df; subclasses implement _run / _run_many / query."""

    backslash_escapes = False  # Redshift treats '\' in string literals as an escape character

    # ---- literals
    def quote(self, s: str) -> str:
        s = s.replace("\x00", "")
        if self.backslash_escapes:
            s = s.replace("\\", "\\\\")
        return "'" + s.replace("'", "''") + "'"

    def literal(self, v) -> str:
        try:
            import numpy as np
        except ImportError:  # pragma: no cover
            np = None
        if _is_missing(v):
            return "NULL"
        if np is not None and isinstance(v, np.generic):
            v = v.item()
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, int):
            return str(v)
        if isinstance(v, float):
            return "NULL" if math.isinf(v) else repr(v)
        if isinstance(v, Decimal):
            return format(v, "f")
        if isinstance(v, pd.Timestamp):
            if v.tzinfo is not None:
                v = v.tz_convert("UTC").tz_localize(None)
            v = v.to_pydatetime()
        if isinstance(v, dt.datetime):
            if v.tzinfo is not None:
                v = v.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return self.quote(v.isoformat(sep=" "))
        if isinstance(v, dt.date):
            return self.quote(v.isoformat())
        if isinstance(v, (list, tuple)) or (np is not None and isinstance(v, np.ndarray)):
            return self.quote(",".join("" if _is_missing(x) else str(x) for x in list(v)))
        if isinstance(v, dict):
            return self.quote(json.dumps(v, default=str, sort_keys=True))
        if isinstance(v, bytes):
            return self.quote(v.decode("utf-8", "replace"))
        return self.quote(str(v))

    # ---- interface
    def execute(self, sql: str) -> None:
        self._run_many([sql])

    def execute_script(self, sql_text: str) -> None:
        statements = split_sql(sql_text)
        if statements:
            self._run_many(statements)

    def insert_df(self, table: str, df: pd.DataFrame) -> int:
        """INSERT all rows of df (column names = table columns). Returns rows inserted."""
        if df is None or df.empty:
            return 0
        cols = list(df.columns)
        head = f"INSERT INTO {table} ({', '.join(cols)}) VALUES "
        statements, rows, size = [], [], len(head)
        for rec in df.itertuples(index=False, name=None):
            row = "(" + ", ".join(self.literal(v) for v in rec) + ")"
            row_bytes = len(row.encode("utf-8")) + 2
            if rows and size + row_bytes > MAX_STATEMENT_BYTES:
                statements.append(head + ",\n".join(rows))
                rows, size = [], len(head)
            rows.append(row)
            size += row_bytes
        if rows:
            statements.append(head + ",\n".join(rows))
        self._run_many(statements)
        return len(df)

    def table_exists(self, name: str) -> bool:
        df = self.query("SELECT COUNT(*) AS n FROM information_schema.tables "
                        f"WHERE table_schema = 'public' AND table_name = {self.quote(name.lower())}")
        return int(df["n"].iloc[0]) > 0

    def columns(self, name: str) -> list[str]:
        df = self.query("SELECT column_name FROM information_schema.columns "
                        f"WHERE table_schema = 'public' AND table_name = {self.quote(name.lower())} "
                        "ORDER BY ordinal_position")
        return [str(c) for c in df["column_name"]]

    def query(self, sql: str) -> pd.DataFrame:  # pragma: no cover - abstract
        raise NotImplementedError

    def _run_many(self, statements: list[str]) -> None:  # pragma: no cover - abstract
        raise NotImplementedError


def _frame(names: list[str], rows: list[list]) -> pd.DataFrame:
    """Build a DataFrame; Decimal columns -> float so the dashboard/tests can do arithmetic."""
    df = pd.DataFrame(rows, columns=names)
    for c in df.columns:
        if df[c].dtype == object:
            sample = df[c].dropna()
            if len(sample) and all(isinstance(x, Decimal) for x in sample.head(50)):
                df[c] = df[c].map(lambda x: None if x is None else float(x)).astype("float64")
    return df


# ---------------------------------------------------------------------------------------------------- Postgres
class PostgresWarehouse(Warehouse):
    def __init__(self):
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL

        url = URL.create("postgresql+psycopg2", username=os.environ.get("PG_USER", "app"),
                         password=os.environ.get("PG_PASSWORD", ""), host=os.environ.get("PG_HOST", "127.0.0.1"),
                         port=int(os.environ.get("PG_PORT", "5432")), database=os.environ.get("PG_DB", "warehouse"))
        self.engine = create_engine(url, pool_pre_ping=True)

    def _run_many(self, statements: list[str]) -> None:
        # raw DB-API cursor without parameters: no ':' / '%' placeholder parsing of our SQL text
        conn = self.engine.raw_connection()
        try:
            cur = conn.cursor()
            for st in statements:
                try:
                    cur.execute(st)
                except Exception as exc:
                    raise RuntimeError(f"SQL failed: {exc}\n--- statement ---\n{st[:2000]}") from exc
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def query(self, sql: str) -> pd.DataFrame:
        conn = self.engine.raw_connection()
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql)
            except Exception as exc:
                raise RuntimeError(f"SQL failed: {exc}\n--- statement ---\n{sql[:2000]}") from exc
            names = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
            conn.commit()
            return _frame(names, [list(r) for r in rows])
        finally:
            conn.close()


# ---------------------------------------------------------------------------------------------------- Redshift
class RedshiftDataWarehouse(Warehouse):
    """Redshift Serverless through the Redshift Data API (asynchronous HTTP API):
       submit statement -> poll describe_statement until FINISHED / FAILED / ABORTED -> fetch result pages."""

    backslash_escapes = True
    POLL_SECONDS = 0.5
    TIMEOUT_SECONDS = 900
    MAX_BATCH = 40  # batch_execute_statement accepts at most 40 SQL statements per call

    def __init__(self):
        import boto3

        self.client = boto3.client("redshift-data", region_name=os.environ.get("AWS_REGION"))
        self.target = dict(WorkgroupName=os.environ["REDSHIFT_WORKGROUP"],
                           Database=os.environ.get("REDSHIFT_DB", "dev"),
                           SecretArn=os.environ["REDSHIFT_SECRET_ARN"])

    def _wait(self, statement_id: str, sql_for_error: str) -> dict:
        """Poll until the statement (or batch) finishes; raise with Redshift's error message if it failed."""
        deadline = time.time() + self.TIMEOUT_SECONDS
        while True:
            d = self.client.describe_statement(Id=statement_id)
            status = d["Status"]  # SUBMITTED | PICKED | STARTED | FINISHED | FAILED | ABORTED
            if status == "FINISHED":
                return d
            if status in ("FAILED", "ABORTED"):
                detail = d.get("Error", status)
                for sub in d.get("SubStatements", []) or []:  # batch: report the sub-statement that failed
                    if sub.get("Status") in ("FAILED", "ABORTED"):
                        detail = f"{sub.get('Error', status)}\n--- statement ---\n{sub.get('QueryString', '')[:2000]}"
                        break
                else:
                    detail = f"{detail}\n--- statement ---\n{sql_for_error[:2000]}"
                raise RuntimeError(f"Redshift statement {status}: {detail}")
            if time.time() > deadline:
                self.client.cancel_statement(Id=statement_id)
                raise RuntimeError(f"Redshift statement timed out after {self.TIMEOUT_SECONDS}s: {sql_for_error[:500]}")
            time.sleep(self.POLL_SECONDS)

    def _run_many(self, statements: list[str]) -> None:
        if len(statements) == 1:
            r = self.client.execute_statement(Sql=statements[0], **self.target)
            self._wait(r["Id"], statements[0])
            return
        # batch_execute_statement runs its statements in ONE transaction, in order (like a script).
        # Scripts longer than 40 statements are sent as consecutive batches.
        for i in range(0, len(statements), self.MAX_BATCH):
            chunk = statements[i:i + self.MAX_BATCH]
            r = self.client.batch_execute_statement(Sqls=chunk, **self.target)
            self._wait(r["Id"], chunk[0])

    @staticmethod
    def _value(field: dict, type_name: str):
        """Convert one Data API field ({'stringValue': ..} / {'longValue': ..} / {'isNull': True} ...) to Python."""
        if field.get("isNull"):
            return None
        if "booleanValue" in field:
            return field["booleanValue"]
        if "longValue" in field:
            return field["longValue"]
        if "doubleValue" in field:
            return field["doubleValue"]
        if "blobValue" in field:
            return field["blobValue"]
        v = field.get("stringValue")
        t = (type_name or "").lower()
        if v is None:
            return None
        if t in ("numeric", "decimal"):
            return Decimal(v)
        if t == "date":
            return dt.date.fromisoformat(v[:10])
        if t.startswith("timestamp"):
            return pd.Timestamp(v)
        if t == "bool":
            return v.lower() in ("t", "true", "1")
        return v

    def query(self, sql: str) -> pd.DataFrame:
        r = self.client.execute_statement(Sql=sql, **self.target)
        d = self._wait(r["Id"], sql)
        if not d.get("HasResultSet"):
            return pd.DataFrame()
        names, types, rows, token = None, None, [], None
        while True:  # results are paginated (NextToken)
            kw = {"Id": r["Id"]}
            if token:
                kw["NextToken"] = token
            page = self.client.get_statement_result(**kw)
            if names is None:
                meta = page["ColumnMetadata"]
                names = [m["name"] for m in meta]
                types = [m.get("typeName", "") for m in meta]
            for rec in page["Records"]:
                rows.append([self._value(f, t) for f, t in zip(rec, types)])
            token = page.get("NextToken")
            if not token:
                break
        return _frame(names or [], rows)


@lru_cache(maxsize=1)
def get_warehouse() -> Warehouse:
    kind = os.environ.get("WAREHOUSE", "postgres").lower()
    if kind == "redshift":
        return RedshiftDataWarehouse()
    if kind == "postgres":
        return PostgresWarehouse()
    raise ValueError(f"WAREHOUSE must be 'postgres' or 'redshift', got {kind!r}")
