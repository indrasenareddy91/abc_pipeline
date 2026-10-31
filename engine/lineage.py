"""Data journey (lineage) + expectation results as small parquet files under DATA_ROOT/ops/.

ops/step_io/batch_id=<b>/step_io_<step>.parquet   one file PER STEP, so bronze and silver share the batch folder
                                                  without overwriting each other (re-running a step replaces its file)
ops/expectations/batch_id=<b>/expectations.parquet

Written with pyarrow (tiny tables; deterministic file names; readable by Spark, pyarrow and engine.gold)."""
from __future__ import annotations

import io as _io
from datetime import datetime, timezone
from pathlib import Path

from . import config, io

STEP_IO_COLUMNS = ["batch_id", "run_id", "step", "table_name", "input_location", "rows_in", "output_location",
                   "rows_out", "rows_rejected", "notes", "recorded_at"]
EXPECTATION_COLUMNS = ["batch_id", "run_id", "table_name", "expectation", "column_name", "severity", "status",
                       "failing_rows", "detail", "checked_at"]


def _schema(kind: str):
    import pyarrow as pa
    s, i, ts = pa.string(), pa.int64(), pa.timestamp("us")
    if kind == "step_io":
        return pa.schema([("batch_id", s), ("run_id", s), ("step", s), ("table_name", s), ("input_location", s),
                          ("rows_in", i), ("output_location", s), ("rows_out", i), ("rows_rejected", i),
                          ("notes", s), ("recorded_at", ts)])
    return pa.schema([("batch_id", s), ("run_id", s), ("table_name", s), ("expectation", s), ("column_name", s),
                      ("severity", s), ("status", s), ("failing_rows", i), ("detail", s), ("checked_at", ts)])


def now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def write_rows(rel_file: str, rows: list[dict], kind: str) -> None:
    """Rows -> one parquet file at DATA_ROOT/rel_file (replaces it)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = _schema(kind)
    table = pa.Table.from_pylist([{c: r.get(c) for c in schema.names} for r in rows], schema=schema)
    buf = _io.BytesIO()
    pq.write_table(table, buf)
    data = buf.getvalue()
    root = config.data_root()
    if io._is_s3(root):
        bucket, key = io._split(root, rel_file)
        io._s3().put_object(Bucket=bucket, Key=key, Body=data)
        return
    target = Path(root) / rel_file
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def step_row(batch_id: str, run_id: str, step: str, table: str, input_location: str, rows_in: int,
             output_location: str, rows_out: int, rows_rejected: int = 0, notes: str = "",
             recorded_at: datetime | None = None) -> dict:
    return {"batch_id": batch_id, "run_id": run_id, "step": step, "table_name": table,
            "input_location": input_location, "rows_in": int(rows_in), "output_location": output_location,
            "rows_out": int(rows_out), "rows_rejected": int(rows_rejected), "notes": notes,
            "recorded_at": recorded_at or now()}


def write_step_io(batch_id: str, step: str, rows: list[dict]) -> None:
    write_rows(f"ops/step_io/batch_id={batch_id}/step_io_{step}.parquet", rows, "step_io")


def write_expectations(batch_id: str, rows: list[dict]) -> None:
    folder = f"ops/expectations/batch_id={batch_id}"
    if io.exists(folder):
        io.delete(folder)
    write_rows(f"{folder}/expectations.parquet", rows, "expectations")
