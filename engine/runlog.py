"""Step logging: one pipeline_runs row per (run, batch, step) + greppable stdout lines.

stdout (CloudWatch alarms match 'ERROR'):
  STEP_START step=gold batch=day1 run=...
  STEP_OK step=gold batch=day1 run=... seconds=12.3 message
  ERROR STEP_FAILED step=gold batch=day1 run=... error=...
"""
from __future__ import annotations

import datetime as dt
import time
import traceback
import uuid
from contextlib import contextmanager

PIPELINE_RUNS_DDL = """CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id VARCHAR(64), batch_id VARCHAR(64), step VARCHAR(64), status VARCHAR(16),
  started_at TIMESTAMP, finished_at TIMESTAMP, message VARCHAR(65535))"""


def new_run_id() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6]


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)


def log(line: str) -> None:
    print(line, flush=True)


def _wh():
    from engine.warehouse import get_warehouse

    return get_warehouse()


def _record(sql: str) -> None:
    """Write to pipeline_runs; a logging failure must never hide the real result of the step."""
    try:
        wh = _wh()
        wh.execute(PIPELINE_RUNS_DDL)
        wh.execute(sql)
    except Exception as exc:  # noqa: BLE001
        log(f"WARN pipeline_runs not written: {str(exc).splitlines()[0][:300]}")


@contextmanager
def step(run_id: str, batch_id: str, name: str):
    """with step(run_id, batch_id, 'gold') as result: ...; result['message'] = 'text shown in pipeline_runs'"""
    result = {"message": ""}
    started = now_utc()
    t0 = time.time()
    log(f"STEP_START step={name} batch={batch_id} run={run_id}")
    try:
        lit = _wh().literal  # backend-specific quoting
    except Exception:  # noqa: BLE001 - warehouse not reachable: _record will print a WARN anyway
        lit = lambda v: "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"  # noqa: E731
    _record("INSERT INTO pipeline_runs (run_id, batch_id, step, status, started_at, finished_at, message) VALUES "
            f"({lit(run_id)}, {lit(batch_id)}, {lit(name)}, 'RUNNING', {lit(started)}, NULL, NULL)")

    def finish(status: str, message: str) -> None:
        _record(f"UPDATE pipeline_runs SET status = {lit(status)}, finished_at = {lit(now_utc())}, "
                f"message = {lit(message[:60000])} WHERE run_id = {lit(run_id)} AND batch_id = {lit(batch_id)} "
                f"AND step = {lit(name)}")

    try:
        yield result
    except BaseException as exc:
        msg = f"{type(exc).__name__}: {exc}"
        log(f"ERROR STEP_FAILED step={name} batch={batch_id} run={run_id} error={msg.splitlines()[0][:500]}")
        log(traceback.format_exc())
        finish("FAILED", msg)
        raise
    secs = round(time.time() - t0, 1)
    log(f"STEP_OK step={name} batch={batch_id} run={run_id} seconds={secs} {result.get('message', '')}".rstrip())
    finish("OK", str(result.get("message", "")))
