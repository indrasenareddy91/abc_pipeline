"""GOLD step: load one batch's silver/ops outputs into the warehouse, maintain the star schema, rebuild KPIs, run checks.

run_gold(batch_id, run_id) does, in order:
  a. project/sql/01_ddl/*.sql            CREATE TABLE IF NOT EXISTS (ops + curated tables)
  b. ops parquet of this batch           -> ingestion_log, profile_columns, rules_log, quarantine, row_counts
                                            (DELETE WHERE batch_id = <batch>, then INSERT: re-running replaces)
  c. silver_changes/<table>/batch_id=<b> -> stg_<table> (dropped + re-created with types from the parquet schema)
  d. project/sql/02_load/*.sql           dims (SCD2), dim_account, fact MERGE  (placeholders replaced, see below)
  e/f. project/sql/03_kpis/*.sql         KPI tables rebuilt; kpi_catalog upserted from each file's header comment;
                                            files with a '-- snapshot:' header save KPI snapshots for this batch
                                            (only when this batch is the newest one, so re-running an old batch
                                            never overwrites newer snapshots)
  g. checks                              built-in + project/sql/04_checks/*.sql (rows returned = violations)
                                            -> check_results; raises if any check fails.
  h. data journey                        step_io gets gold's own rows (silver_changes -> stg_*, stg_* -> dim_*/fact_*,
                                            curated -> kpi tables, with row counts); lineage is read from the SQL files
  i. reports                             engine.reports writes DATA_ROOT/reports/<batch>/ (+ latest/) Markdown + CSV

SQL placeholders: {batch_id} {batch_date} {as_of_date} {first_batch} and every scalar setting in pipeline.yaml
(e.g. {suspicious_amount}); unknown placeholders raise an error before anything runs.
"""
from __future__ import annotations

import datetime as dt
import io as _io
import re
from pathlib import Path

import pandas as pd

from engine import config
from engine import io as storage
from engine.runlog import log, now_utc
from engine.warehouse import Warehouse, get_warehouse

# Settings gold needs; used when pipeline.yaml does not define them (values from the brief / SPEC).
DEFAULT_SETTINGS = {
    "suspicious_amount": 150000,        # KPI 5: brief left the number blank -> assumption
    "suspicious_burst_count": 3,        # KPI 5: >= 3 transactions on one account ...
    "suspicious_burst_minutes": 10,     # ... within 10 minutes (brief)
    "suspicious_night_start_hour": 0,   # KPI 5: transaction time 00:00 <= t < 05:00
    "suspicious_night_end_hour": 5,
    "dormant_days": 90,                 # KPI 4
    "kpi_top_n": 5,                     # KPI 1
}
# fact table -> silver table it must match row-for-row (built-in check)
DEFAULT_FACT_SOURCES = {"fact_transaction": "transactions"}
# warehouse ops table -> folder under DATA_ROOT (partitioned by batch_id=<batch>)
OPS_SOURCES = {"ingestion_log": "ops/ingestion_log", "profile_columns": "ops/profile",
               "rules_log": "ops/rules_log", "row_counts": "ops/row_counts",
               "step_io": "ops/step_io", "dq_expectations": "ops/expectations"}
STEP_IO_COLUMNS = ["batch_id", "run_id", "step", "table_name", "input_location", "rows_in", "output_location",
                   "rows_out", "rows_rejected", "notes", "recorded_at"]
# Generic ops tables every project gets (same text as project/sql/01_ddl/01_ops.sql; IF NOT EXISTS = harmless twice)
ENGINE_DDL = """CREATE TABLE IF NOT EXISTS step_io (
  batch_id VARCHAR(64), run_id VARCHAR(64), step VARCHAR(32), table_name VARCHAR(128),
  input_location VARCHAR(2048), rows_in BIGINT, output_location VARCHAR(2048), rows_out BIGINT,
  rows_rejected BIGINT, notes VARCHAR(65535), recorded_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS dq_expectations (
  batch_id VARCHAR(64), run_id VARCHAR(64), table_name VARCHAR(128), expectation VARCHAR(1024),
  column_name VARCHAR(256), severity VARCHAR(16), status VARCHAR(8), failing_rows BIGINT,
  detail VARCHAR(65535), checked_at TIMESTAMP);"""
WAREHOUSE_PREFIX = "warehouse."  # step_io locations of warehouse tables: warehouse.<table>

_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


# ------------------------------------------------------------------------------------------------ parquet reading
def read_parquet_dir(rel: str):
    """All *.parquet files directly under DATA_ROOT/rel as one pyarrow Table, or None if there are none.
    Works for a local DATA_ROOT and for s3:// (engine.io downloads the objects with boto3)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not storage.exists(rel):
        return None
    files = [f for f in storage.list_files(rel) if f.endswith(".parquet")]
    if not files:
        return None
    tables = [pq.read_table(_io.BytesIO(storage.read_bytes(f"{rel}/{f}"))) for f in files]
    return tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="default")


def parquet_rows(rel: str) -> int | None:
    import pyarrow.parquet as pq

    if not storage.exists(rel):
        return None
    files = [f for f in storage.list_files(rel) if f.endswith(".parquet")]
    if not files:
        return None
    return sum(pq.ParquetFile(_io.BytesIO(storage.read_bytes(f"{rel}/{f}"))).metadata.num_rows for f in files)


def to_pandas(tbl) -> pd.DataFrame:
    # ints with nulls stay Python ints (not float), dates stay datetime.date, decimals stay Decimal
    return tbl.to_pandas(integer_object_nulls=True, date_as_object=True)


def sql_type(arrow_type) -> str:
    """Postgres + Redshift column type for a pyarrow type."""
    import pyarrow.types as t

    if t.is_boolean(arrow_type):
        return "BOOLEAN"
    if t.is_integer(arrow_type):
        return "BIGINT"
    if t.is_floating(arrow_type):
        return "DOUBLE PRECISION"
    if t.is_decimal(arrow_type):
        return f"DECIMAL({min(arrow_type.precision, 38)},{arrow_type.scale})"
    if t.is_date(arrow_type):
        return "DATE"
    if t.is_timestamp(arrow_type):
        return "TIMESTAMP"
    return "VARCHAR(65535)"


# ------------------------------------------------------------------------------------------------ settings / SQL
def _scalar(v) -> str:
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()[:10] if not isinstance(v, dt.datetime) else v.isoformat(sep=" ")
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    return str(v)


def gold_settings() -> dict:
    s = dict(DEFAULT_SETTINGS)
    cfg = config.settings() or {}
    if "suspicious_amount" not in cfg and "suspicious_high_value" in cfg:  # older config name
        s["suspicious_amount"] = cfg["suspicious_high_value"]
    s.update({k: v for k, v in cfg.items() if v is not None and not isinstance(v, (dict, list))})
    s["as_of_date"] = config.as_of_date().isoformat()
    return s


def batch_date_for(batch_id: str) -> str:
    """Business date of the batch (settings.batch_dates); fallback: the batch id if it is a date, else as_of_date."""
    d = config.batch_date(batch_id)
    if d is not None:
        return d.isoformat()
    try:
        return dt.date.fromisoformat(batch_id[:10]).isoformat()
    except ValueError:
        fallback = config.as_of_date().isoformat()
        log(f"WARN batch_dates has no entry for batch={batch_id}; using as_of_date {fallback} as its business date")
        return fallback


def render(sql_text: str, params: dict, source: str = "") -> str:
    missing = sorted({m for m in _PLACEHOLDER.findall(sql_text) if m not in params})
    if missing:
        raise ValueError(f"{source}: unknown placeholders {missing}; known: {sorted(params)}")
    return _PLACEHOLDER.sub(lambda m: _scalar(params[m.group(1)]), sql_text)


def sql_files(folder: str) -> list[Path]:
    d = Path(config.project_dir()) / "sql" / folder
    return sorted(d.glob("*.sql")) if d.is_dir() else []


def parse_header(sql_text: str) -> dict:
    """'-- key: value' lines at the top of a file (continuation lines '--   more text' are appended)."""
    meta, last = {}, None
    for line in sql_text.splitlines():
        s = line.strip()
        if not s:
            continue
        if not s.startswith("--"):
            break
        body = s[2:].strip()
        m = re.match(r"^([a-z_]+):\s*(.*)$", body)
        if m:
            last = m.group(1)
            meta[last] = m.group(2).strip()
        elif last and body:
            meta[last] = (meta[last] + " " + body).strip()
    return meta


# ------------------------------------------------------------------------------------------------ steps
def run_ddl(wh: Warehouse, params: dict) -> None:
    for f in sql_files("01_ddl"):
        wh.execute_script(render(f.read_text(), params, f.name))
    wh.execute_script(ENGINE_DDL)


def _align(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Keep the warehouse table's columns in its order; missing columns -> NULL; extra columns dropped."""
    out = pd.DataFrame(index=df.index)
    for c in cols:
        out[c] = df[c] if c in df.columns else None
    return out


def load_ops(wh: Warehouse, batch_id: str) -> dict:
    counts = {}
    sources = dict(OPS_SOURCES)
    frames = {}
    for table, rel in sources.items():
        tbl = read_parquet_dir(f"{rel}/batch_id={batch_id}")
        frames[table] = to_pandas(tbl) if tbl is not None else None
    q = [to_pandas(t) for t in (read_parquet_dir(f"quarantine/{name}/batch_id={batch_id}")
                                for name in config.tables()) if t is not None]
    frames["quarantine"] = pd.concat(q, ignore_index=True) if q else None
    for table, df in frames.items():
        wh.execute(f"DELETE FROM {table} WHERE batch_id = {wh.quote(batch_id)}")
        if df is None:
            log(f"WARN no {table} parquet for batch={batch_id}")
            counts[table] = 0
            continue
        df = df.copy()
        df["batch_id"] = batch_id  # partition column is not stored inside Spark's parquet files
        counts[table] = wh.insert_df(table, _align(df, wh.columns(table)))
    return counts


def load_staging(wh: Warehouse, batch_id: str) -> dict:
    counts = {}
    for name in config.tables():
        stg = f"stg_{name}"
        tbl = read_parquet_dir(f"silver_changes/{name}/batch_id={batch_id}")
        if tbl is None:  # nothing changed for this table in this batch -> empty staging with the silver schema
            snap = read_parquet_dir(f"silver/{name}")
            if snap is None:
                if wh.table_exists(stg):
                    wh.execute(f"DELETE FROM {stg}")
                    counts[name] = 0
                    continue
                raise FileNotFoundError(f"no silver_changes or silver parquet for table {name}")
            tbl = snap.slice(0, 0)
        cols = [f for f in tbl.schema if f.name != "batch_id"]
        ddl = ",\n  ".join(f"{f.name.lower()} {sql_type(f.type)}" for f in cols)
        wh.execute_script(f"DROP TABLE IF EXISTS {stg};\nCREATE TABLE {stg} (\n  {ddl});")
        df = to_pandas(tbl.select([f.name for f in cols]))
        df.columns = [c.lower() for c in df.columns]
        counts[name] = wh.insert_df(stg, df)
    return counts


def run_load_sql(wh: Warehouse, params: dict) -> list[str]:
    done = []
    for f in sql_files("02_load"):
        wh.execute_script(render(f.read_text(), params, f.name))
        done.append(f.stem)
    return done


def run_kpis(wh: Warehouse, params: dict, is_latest_batch: bool) -> list[str]:
    built = []
    for f in sql_files("03_kpis"):
        text = f.read_text()
        meta = parse_header(text)
        if "snapshot" in meta:
            if is_latest_batch:
                wh.execute_script(render(text, params, f.name))
            else:
                log(f"SNAPSHOT_SKIPPED file={f.name}: batch {params['batch_id']} is older than the newest loaded batch")
            continue
        wh.execute_script(render(text, params, f.name))
        tables = list(dict.fromkeys(re.findall(r"CREATE\s+TABLE\s+([A-Za-z0-9_]+)", text, flags=re.I)))
        kpi_id = meta.get("kpi_id") or f.stem.split("_")[0]
        built += tables
        row = pd.DataFrame([{"kpi_id": kpi_id, "title": meta.get("title", f.stem), "question": meta.get("question"),
                             "logic": meta.get("logic"), "assumptions": meta.get("assumptions"),
                             "table_name": ",".join(tables)}])
        wh.execute(f"DELETE FROM kpi_catalog WHERE kpi_id = {wh.quote(kpi_id)}")
        wh.insert_df("kpi_catalog", row)
    return built


def _violations(wh: Warehouse, sql: str) -> tuple[int, str]:
    df = wh.query(sql)
    if df.empty:
        return 0, ""
    return len(df), df.head(5).to_string(index=False)[:3000]


def run_checks(wh: Warehouse, batch_id: str, run_id: str, params: dict) -> list[dict]:
    b = wh.quote(batch_id)
    checks: list[tuple[str, str]] = [
        ("row_counts_reconcile",
         f"SELECT table_name, raw_rows, exact_duplicates, key_duplicates, quarantined, accepted, inserted, updated, "
         f"unchanged FROM row_counts WHERE batch_id = {b} AND (raw_rows <> exact_duplicates + key_duplicates + "
         f"quarantined + accepted OR accepted <> inserted + updated + unchanged)"),
        ("quarantine_has_reason",
         f"SELECT table_name, record_key, rule_id FROM quarantine WHERE batch_id = {b} "
         f"AND (reason IS NULL OR TRIM(reason) = '' OR rule_id IS NULL)"),
        ("quarantine_matches_row_counts",
         f"SELECT r.table_name, r.quarantined, COALESCE(q.n, 0) AS quarantine_rows FROM row_counts r "
         f"LEFT JOIN (SELECT table_name, COUNT(*) AS n FROM quarantine WHERE batch_id = {b} GROUP BY table_name) q "
         f"ON q.table_name = r.table_name WHERE r.batch_id = {b} AND r.quarantined <> COALESCE(q.n, 0)"),
    ]
    for name, spec in config.tables().items():
        key = (spec or {}).get("key")
        if key and wh.table_exists(f"stg_{name}"):
            checks.append((f"stg_{name}_key_unique_not_null",
                           f"SELECT {key}, COUNT(*) AS n FROM stg_{name} GROUP BY {key} "
                           f"HAVING COUNT(*) > 1 OR {key} IS NULL"))
    results = []
    for name, sql in checks:
        n, detail = _violations(wh, sql)
        results.append({"check_name": name, "status": "PASS" if n == 0 else "FAIL",
                        "detail": f"{n} violation(s)" + (f"\n{detail}" if detail else "")})
    # fact rows = silver snapshot rows
    fact_sources = (config.settings() or {}).get("gold_fact_sources") or DEFAULT_FACT_SOURCES
    for fact, silver in fact_sources.items():
        silver_n = parquet_rows(f"silver/{silver}")
        fact_n = int(wh.query(f"SELECT COUNT(*) AS n FROM {fact}")["n"].iloc[0])
        ok = silver_n is not None and silver_n == fact_n
        results.append({"check_name": f"{fact}_rows_equal_silver_{silver}", "status": "PASS" if ok else "FAIL",
                        "detail": f"{fact}={fact_n} rows, silver/{silver}={silver_n} rows"})
    # project checks
    for f in sql_files("04_checks"):
        meta = parse_header(f.read_text())
        n, detail = _violations(wh, render(f.read_text(), params, f.name).strip().rstrip(";"))
        results.append({"check_name": f.stem, "status": "PASS" if n == 0 else "FAIL",
                        "detail": (meta.get("check", "") + f" | {n} violation(s)" + (f"\n{detail}" if detail else ""))
                        .strip(" |")})
    now = now_utc()
    df = pd.DataFrame([{"run_id": run_id, "batch_id": batch_id, **r, "checked_at": now} for r in results])
    wh.insert_df("check_results", df[["run_id", "batch_id", "check_name", "status", "detail", "checked_at"]])
    for r in results:
        log(f"CHECK {r['status']} {r['check_name']}" + ("" if r["status"] == "PASS" else
                                                         f" -> {r['detail'].splitlines()[0]}"))
    return results


# ------------------------------------------------------------------------------------------------ data journey
_SQL_TARGET = re.compile(r"\b(?:INSERT\s+INTO|MERGE\s+INTO|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|UPDATE)\s+"
                         r"([A-Za-z_][A-Za-z0-9_]*)", re.I)
_SQL_SOURCE = re.compile(r"\b(?:FROM|JOIN|USING)\s+([A-Za-z_][A-Za-z0-9_]*)", re.I)
_SQL_DELETE = re.compile(r"\bDELETE\s+FROM\s+[A-Za-z_][A-Za-z0-9_]*", re.I)


def sql_lineage(sql_text: str, known_tables: set[str]) -> tuple[list[str], list[str]]:
    """(tables written, tables read) by a SQL file. Work tables (wrk_*) are skipped; reads are limited to
    tables that exist in the warehouse (drops aliases / CTE names); a table both read and written counts as written."""
    body = "\n".join(line for line in sql_text.splitlines() if not line.strip().startswith("--"))
    targets = [t.lower() for t in _SQL_TARGET.findall(body)]
    targets = list(dict.fromkeys(t for t in targets if not t.startswith("wrk_")))
    body = _SQL_DELETE.sub(" ", body)
    sources = [t.lower() for t in _SQL_SOURCE.findall(body)]
    sources = list(dict.fromkeys(t for t in sources if t in known_tables and t not in targets
                                 and not t.startswith("wrk_")))
    return targets, sources


def warehouse_tables(wh: Warehouse) -> set[str]:
    df = wh.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    return {str(t).lower() for t in df["table_name"]} if not df.empty else set()


def count_rows(wh: Warehouse, tables: list[str]) -> dict[str, int]:
    """Row counts of many tables in a few UNION ALL queries (one round trip per 25 tables on Redshift)."""
    out: dict[str, int] = {}
    tables = list(dict.fromkeys(tables))
    for i in range(0, len(tables), 25):
        part = tables[i:i + 25]
        sql = " UNION ALL ".join(f"SELECT {wh.quote(t)} AS t, COUNT(*) AS n FROM {t}" for t in part)
        for _, r in wh.query(sql).iterrows():
            out[str(r["t"])] = int(r["n"])
    return out


def _derived_upstream_rows(wh: Warehouse, batch_id: str, run_id: str, now) -> list[dict]:
    """bronze/silver step_io rows rebuilt from ingestion_log / row_counts when those steps wrote no step_io parquet
    (older engine). Marked in notes so nobody mistakes them for the step's own record."""
    b = wh.quote(batch_id)
    have = set(wh.query(f"SELECT DISTINCT step FROM step_io WHERE batch_id = {b}")["step"].astype(str)) \
        if wh.table_exists("step_io") else set()
    rows = []
    if "bronze" not in have:
        ing = wh.query(f"SELECT table_name, file_name, rows_in FROM ingestion_log WHERE batch_id = {b} "
                       "ORDER BY table_name, file_name")
        for _, r in ing.iterrows():
            f = str(r["file_name"])
            rows.append({"step": "bronze", "table_name": r["table_name"],
                         "input_location": f if f.startswith("raw/") else f"raw/{batch_id}/{f}",
                         "rows_in": r["rows_in"], "output_location": f"bronze/{r['table_name']}/_batch_id={batch_id}/",
                         "rows_out": r["rows_in"], "rows_rejected": 0,
                         "notes": "copied as received (all text); derived by gold from ingestion_log"})
    if "silver" not in have:
        rc = wh.query(f"SELECT * FROM row_counts WHERE batch_id = {b} ORDER BY table_name")
        for _, r in rc.iterrows():
            t = r["table_name"]
            rows.append({"step": "silver", "table_name": t, "input_location": f"bronze/{t}/_batch_id={batch_id}/",
                         "rows_in": r["raw_rows"], "output_location": f"silver/{t}/", "rows_out": r["accepted"],
                         "rows_rejected": r["quarantined"],
                         "notes": f"{int(r['exact_duplicates'] or 0)} exact duplicate(s) and "
                                  f"{int(r['key_duplicates'] or 0)} older copy(ies) of the same key dropped; "
                                  f"rejected rows -> quarantine/{t}/batch_id={batch_id}/; "
                                  "derived by gold from row_counts"})
    for r in rows:
        r.update({"batch_id": batch_id, "run_id": run_id, "recorded_at": now})
    return rows


def record_step_io(wh: Warehouse, batch_id: str, run_id: str, stg_counts: dict) -> int:
    """Gold's own step_io rows: one row per input -> output edge, with the row counts after this run.
    rows_in = rows in the input (for stg_* = rows this batch brought), rows_out = rows now in the output table."""
    now = now_utc()
    known = warehouse_tables(wh)
    rows = _derived_upstream_rows(wh, batch_id, run_id, now)
    stg_now = count_rows(wh, [f"stg_{name}" for name in stg_counts if f"stg_{name}" in known])
    for name in stg_counts:
        rel = f"silver_changes/{name}/batch_id={batch_id}"
        rows.append({"step": "gold", "table_name": name, "input_location": f"{rel}/",
                     "rows_in": parquet_rows(rel) or 0, "output_location": f"{WAREHOUSE_PREFIX}stg_{name}",
                     "rows_out": stg_now.get(f"stg_{name}"), "rows_rejected": 0,
                     "notes": "rows this batch inserted or changed in silver (staging, replaced every batch)"})
    edges = []
    for folder, step_name in (("02_load", "gold"), ("03_kpis", "kpis")):
        for f in sql_files(folder):
            targets, sources = sql_lineage(f.read_text(), known)
            for t in targets:
                if t not in known:
                    continue
                for src in sources or [None]:
                    edges.append((step_name, f"{folder}/{f.name}", src, t))
    counts = count_rows(wh, [t for e in edges for t in (e[2], e[3]) if t])
    for step_name, fname, src, t in edges:
        rows.append({"step": step_name, "table_name": t,
                     "input_location": f"{WAREHOUSE_PREFIX}{src}" if src else None,
                     "rows_in": counts.get(src) if src else None,
                     "output_location": f"{WAREHOUSE_PREFIX}{t}", "rows_out": counts.get(t), "rows_rejected": 0,
                     "notes": f"{fname}; rows_out = all rows now in {t}"})
    for r in rows:
        r.setdefault("batch_id", batch_id)
        r.setdefault("run_id", run_id)
        r.setdefault("recorded_at", now)
    wh.execute(f"DELETE FROM step_io WHERE batch_id = {wh.quote(batch_id)} AND step IN ('gold', 'kpis')")
    df = pd.DataFrame(rows, columns=STEP_IO_COLUMNS)
    for c in ("rows_in", "rows_out", "rows_rejected"):
        df[c] = df[c].map(lambda v: None if v is None or pd.isna(v) else int(v)).astype(object)
    return wh.insert_df("step_io", df)


def run_gold(batch_id: str, run_id: str) -> dict:
    wh = get_warehouse()
    params = gold_settings()
    params.update({"batch_id": batch_id, "batch_date": batch_date_for(batch_id), "run_id": run_id})
    params.setdefault("first_batch", batch_id)

    run_ddl(wh, params)
    ops = load_ops(wh, batch_id)
    first = wh.query("SELECT MIN(batch_id) AS b, MAX(batch_id) AS m FROM row_counts")
    params["first_batch"] = first["b"].iloc[0] or batch_id
    latest = first["m"].iloc[0] or batch_id
    log(f"GOLD batch={batch_id} batch_date={params['batch_date']} as_of_date={params['as_of_date']} "
        f"first_batch={params['first_batch']} ops={ops}")
    stg = load_staging(wh, batch_id)
    log(f"GOLD staging rows {stg}")
    loaded = run_load_sql(wh, params)
    kpis = run_kpis(wh, params, is_latest_batch=batch_id >= latest)
    log(f"GOLD kpi tables rebuilt: {', '.join(kpis)}")
    results = run_checks(wh, batch_id, run_id, params)
    failed = [r["check_name"] for r in results if r["status"] != "PASS"]
    journey = record_step_io(wh, batch_id, run_id, stg)
    log(f"GOLD step_io rows recorded: {journey}")
    try:  # reports are written even when a check failed (that is when they are needed most)
        from engine.reports import write_reports

        reports = write_reports(wh, batch_id, is_latest_batch=batch_id >= latest)
        log(f"REPORTS_OK batch={batch_id} files={len(reports)}")
    except Exception as exc:  # noqa: BLE001 - a report problem must not hide the load result
        reports = []
        log(f"WARN REPORTS_FAILED batch={batch_id}: {type(exc).__name__}: {exc}")
    summary = {"ops": ops, "staging": stg, "load_sql": loaded, "kpi_tables": kpis,
               "checks_passed": len(results) - len(failed), "checks_failed": failed,
               "step_io_rows": journey, "reports": len(reports)}
    if failed:
        raise RuntimeError(f"{len(failed)} check(s) failed: {', '.join(failed)} (see check_results)")
    return summary
