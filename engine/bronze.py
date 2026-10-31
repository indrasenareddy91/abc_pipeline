"""BRONZE: land every raw file of a batch exactly as received (all text), plus lineage columns.

raw/<batch>/<files>  (or raw/ if the batch folder is missing)
  -> bronze/<table>/_batch_id=<batch>/      (only this batch's partition is overwritten)
  -> ops/ingestion_log/batch_id=<batch>/    one row per file (rows, columns, sha256)
  -> ops/profile/batch_id=<batch>/          EDA per column (blanks, placeholders, distinct, shapes, top values)
  -> ops/step_io/batch_id=<batch>/step_io_bronze.parquet   data journey: one row per file (raw file -> bronze)
`file:` in pipeline.yaml may be one glob or a list of globs; every matching file lands in the same table
(record key = <file>#<row>, unique per file).

Files are decoded in Python (any odd CSV/JSON, incl. BROKEN JSON salvaged record by record) and turned into a
DataFrame of strings; everything after that is Spark. Nothing is trimmed, cast or dropped here."""
from __future__ import annotations

import csv
import fnmatch
import hashlib
import io as _io
import json
from datetime import datetime, timezone
from pathlib import PurePosixPath

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import config, io, lineage

META_COLS = ["_source_file", "_source_row_number", "_batch_id", "_loaded_at", "_parse_error", "_raw_text"]


# ---------- file -> table ----------
def raw_folder(batch_id: str) -> str:
    """Files live in raw/<batch>/ (e.g. raw/day1/) or directly in raw/."""
    sub = f"raw/{batch_id}"
    return sub if io.list_files(sub) else "raw"


def table_for_file(name: str) -> str | None:
    """First table (config order) whose `file` glob - or any glob of a `file` list - matches the file name."""
    for table, spec in config.tables().items():
        if any(fnmatch.fnmatch(name.lower(), p.lower()) for p in config.file_patterns(table, spec)):
            return table
    return None


# ---------- decoding (exactly as received, all strings) ----------
def _decode(data: bytes, name: str) -> str:
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        print(f"WARN ENCODING file={name} not UTF-8 -> read as cp1252")
        return data.decode("cp1252", errors="replace")


def _to_text(v) -> str:
    """JSON value -> text as sent: null -> '', true -> 'true', objects/lists -> JSON."""
    if v is None:
        return ""
    if isinstance(v, (bool, dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def salvage_json(text: str, name: str) -> list[dict]:
    """Read a broken JSON array object by object. Unreadable pieces become records with _parse_error and the
    raw text (silver quarantines them), so nothing is dropped silently."""
    dec, i, out = json.JSONDecoder(), 0, []
    while True:
        j = text.find("{", i)
        if j < 0:
            break
        try:
            obj, i = dec.raw_decode(text, j)
            out.append(obj if isinstance(obj, dict) else {"_parse_error": "not an object", "_raw_text": str(obj)})
        except json.JSONDecodeError as exc:
            end = text.find("}", j)
            end = len(text) if end < 0 else end + 1
            out.append({"_parse_error": str(exc), "_raw_text": text[j:end][:2000]})
            i = end
    bad = sum(1 for r in out if "_parse_error" in r)
    print(f"WARN JSON_SALVAGED file={name} records={len(out)} unreadable={bad}")
    return out


def parse_file(name: str, data: bytes, read: dict | None = None) -> tuple[list[str], list[dict]]:
    """-> (business columns in file order, records as {column: text} incl. optional _parse_error/_raw_text).
    read (per table, optional): {sep: "\\t" | "|" | ",", header: false, names: [col1, col2, ...]} for files like a
    tab-separated .txt without a header row."""
    read = read or {}
    text = _decode(data, name)
    suffix = PurePosixPath(name).suffix.lower()
    if suffix in (".json", ".jsonl", ".ndjson"):
        if suffix != ".json":
            records = salvage_json("[" + ",".join(l for l in text.splitlines() if l.strip()) + "]", name)
        else:
            try:
                records = json.loads(text)
            except json.JSONDecodeError as exc:
                print(f"WARN JSON_BROKEN file={name} error={exc} -> reading record by record")
                records = salvage_json(text, name)
        if isinstance(records, dict):  # {"products": [...]} or a single object
            lists = [v for v in records.values() if isinstance(v, list)]
            records = lists[0] if lists else [records]
        columns: list[str] = []
        for r in records:
            for k in r:
                if k not in columns and k not in ("_parse_error", "_raw_text"):
                    columns.append(k)
        out = []
        for r in records:
            rec = {c: _to_text(r.get(c)) if "_parse_error" not in r else None for c in columns}
            rec["_parse_error"] = r.get("_parse_error")
            rec["_raw_text"] = r.get("_raw_text")
            out.append(rec)
        return [str(c).strip() for c in columns], [{str(k).strip(): v for k, v in r.items()} for r in out]

    sep = read.get("sep") or ("\t" if suffix in (".tsv", ".txt") else ",")
    sep = {"tab": "\t", "\\t": "\t", "pipe": "|"}.get(sep, sep)
    rows = [r for r in csv.reader(_io.StringIO(text, newline=""), delimiter=sep) if any(x.strip() for x in r)]
    if not rows:
        return [], []
    if read.get("header", True) is False:   # no header row in the file: column names come from the config
        header, body = [str(n) for n in read.get("names") or []], rows
    else:
        header, body = [h.strip() for h in rows[0]], rows[1:]
    out = []
    for r in body:
        rec = dict(zip(header, r + [""] * (len(header) - len(r))))
        rec["_parse_error"], rec["_raw_text"] = None, None
        if len(r) > len(header):  # more fields than header: keep the record, let silver quarantine it
            rec["_parse_error"] = f"row has {len(r)} fields, header has {len(header)}"
            rec["_raw_text"] = sep.join(r)
        out.append(rec)
    return header, out


def schema_check(table: str, columns: list[str], file_name: str) -> None:
    """Stop the run if a configured (non-derived, non-optional) column is missing from the file."""
    spec = (config.tables().get(table) or {}).get("columns") or {}
    required = [c for c, s in spec.items() if not (s or {}).get("derived") and not (s or {}).get("optional")]
    renamed = {v: k for k, v in ((config.tables().get(table) or {}).get("rename") or {}).items()}  # clean -> source
    missing = [c for c in required if c not in columns and renamed.get(c) not in columns]
    if missing:
        raise ValueError(f"Schema check failed for table '{table}' file '{file_name}': missing column(s) {missing}. "
                         f"File has {columns}. Fix the file or the config (columns.<name>.optional: true).")
    src_names = (config.tables().get(table) or {}).get("rename") or {}
    extra = [c for c in columns if spec and c not in spec and c not in src_names]
    if extra:
        print(f"WARN SCHEMA_EXTRA table={table} file={file_name} columns kept in bronze only: {extra}")


# ---------- profile (EDA) ----------
def profile(df: DataFrame, table: str, batch_id: str) -> DataFrame:
    """One row per business column: rows_total, blank, placeholder, distinct_values, untrimmed, top_values, shapes."""
    cols = [c for c in df.columns if not c.startswith("_")]
    ph = [p for p in config.placeholders() if p != ""]
    melted = df.select(F.explode(F.array(*[
        F.struct(F.lit(i).alias("pos"), F.lit(c).alias("column_name"), F.coalesce(F.col(f"`{c}`"), F.lit("")).alias("v"))
        for i, c in enumerate(cols)])).alias("x")).select("x.*")
    trimmed = F.regexp_replace("v", r"^\s+|\s+$", "")
    melted = melted.select("pos", "column_name", "v", F.lower(trimmed).alias("norm"),
                           F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(
                               trimmed, "[0-9]", "9"), "[A-Za-z]", "A"), "9+", "9"), "A+", "A").alias("shape"))
    base = melted.groupBy("pos", "column_name").agg(
        F.count(F.lit(1)).alias("rows_total"),
        F.sum(F.when(F.col("norm") == "", 1).otherwise(0)).alias("blank"),
        F.sum(F.when(F.col("norm").isin(ph), 1).otherwise(0)).alias("placeholder"),
        F.countDistinct("norm").alias("distinct_values"),
        F.sum(F.when(F.col("v") != trimmed, 1).otherwise(0)).alias("untrimmed"))

    def top(col: str, n: int, alias: str) -> DataFrame:
        counts = melted.groupBy("pos", col).count()
        agg = counts.groupBy("pos").agg(F.slice(F.sort_array(F.collect_list(
            F.struct((-F.col("count")).alias("neg"), F.col(col).alias("val")))), 1, n).alias("items"))
        return agg.select("pos", F.array_join(F.transform(
            "items", lambda x: F.concat(F.lit("'"), x["val"], F.lit("':"), (-x["neg"]).cast("string"))), "; ").alias(alias))

    out = base.join(top("norm", 8, "top_values"), "pos").join(top("shape", 6, "shapes"), "pos")
    return out.orderBy("pos").select(
        F.lit(batch_id).alias("batch_id"), F.lit(table).alias("table_name"), "column_name",
        *[F.col(c).cast("long").alias(c) for c in ("rows_total", "blank", "placeholder", "distinct_values", "untrimmed")],
        "top_values", "shapes")


# ---------- main ----------
def run_bronze(spark: SparkSession, batch_id: str, run_id: str) -> dict:
    print(f"STEP_START step=bronze batch={batch_id} run_id={run_id}")
    folder = raw_folder(batch_id)
    files = io.list_files(folder)
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    per_table: dict[str, list[tuple[str, list[str], list[dict]]]] = {}
    log_rows, io_rows = [], []
    for name in files:
        table = table_for_file(name)
        if table is None:
            print(f"WARN SKIP_FILE {folder}/{name} (matches no table 'file' pattern in pipeline.yaml)")
            continue
        data = io.read_bytes(f"{folder}/{name}")
        columns, records = parse_file(name, data, (config.tables()[table] or {}).get("read"))
        schema_check(table, columns, name)
        per_table.setdefault(table, []).append((f"{folder}/{name}", columns, records))
        log_rows.append((batch_id, table, f"{folder}/{name}", len(records), ",".join(columns),
                         hashlib.sha256(data).hexdigest(), loaded_at, run_id))
        unreadable = sum(1 for r in records if r.get("_parse_error"))
        io_rows.append(lineage.step_row(
            batch_id, run_id, "bronze", table, f"{folder}/{name}", len(records),
            f"bronze/{table}/_batch_id={batch_id}/", len(records), 0,
            f"{len(columns)} columns, stored as text exactly as received"
            + (f"; {unreadable} unreadable record(s) kept with their raw text (silver quarantines them)"
               if unreadable else ""), loaded_at))

    summary, profiles = {}, []
    for table in config.tables():
        target = f"bronze/{table}/_batch_id={batch_id}"
        if table not in per_table:
            if io.exists(target):  # a re-run without that file: drop the stale partition (rerun = replace)
                io.delete(target)
            continue
        columns: list[str] = []
        for _, cols, _ in per_table[table]:
            columns += [c for c in cols if c not in columns]
        rows = []
        for source, _, records in per_table[table]:
            for i, r in enumerate(records, 1):
                rows.append(tuple(r.get(c) for c in columns)  # column absent in this file -> NULL
                            + (source, i, batch_id, loaded_at, r.get("_parse_error"), r.get("_raw_text")))
        schema = T.StructType([T.StructField(c, T.StringType()) for c in columns] + [
            T.StructField("_source_file", T.StringType()), T.StructField("_source_row_number", T.IntegerType()),
            T.StructField("_batch_id", T.StringType()), T.StructField("_loaded_at", T.TimestampType()),
            T.StructField("_parse_error", T.StringType()), T.StructField("_raw_text", T.StringType())])
        df = spark.createDataFrame(rows, schema)
        df.coalesce(1).write.mode("overwrite").parquet(io.path(target))
        profiles.append(profile(spark.read.parquet(io.path(target)), table, batch_id))
        summary[table] = len(rows)

    log_schema = ("batch_id string, table_name string, file_name string, rows_in long, columns_in string, "
                  "sha256 string, loaded_at timestamp, run_id string")
    spark.createDataFrame(log_rows, log_schema).coalesce(1).write.mode("overwrite") \
        .parquet(io.path(f"ops/ingestion_log/batch_id={batch_id}"))
    if profiles:
        prof = profiles[0]
        for p in profiles[1:]:
            prof = prof.unionByName(p)
        prof.coalesce(1).write.mode("overwrite").parquet(io.path(f"ops/profile/batch_id={batch_id}"))
    lineage.write_step_io(batch_id, "bronze", io_rows)   # data journey: raw file -> bronze partition
    msg = ", ".join(f"{t}={n}" for t, n in summary.items()) or f"no files in {folder}/"
    print(f"STEP_OK step=bronze batch={batch_id} run_id={run_id} files={len(log_rows)} rows: {msg}")
    return summary
