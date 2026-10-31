"""Run the pipeline locally for one batch:  python jobs/run_local.py --batch day1 [--steps bronze,silver,gold]

Defaults (only when not already set): RUN_MODE=local, PROJECT_DIR=<repo>/project, DATA_ROOT=<repo>/data,
WAREHOUSE=postgres, PG_* = local Postgres. If DATA_ROOT/raw/<batch> does not exist yet, the batch is copied from
project/sample_data/raw/<batch> (simulates the file drop). One run_id per invocation; every step is logged to
pipeline_runs + stdout (STEP_START / STEP_OK / ERROR STEP_FAILED). Exit code 1 if any step fails."""
import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DEFAULTS = {"RUN_MODE": "local", "PROJECT_DIR": str(ROOT / "project"), "DATA_ROOT": str(ROOT / "data"),
            "WAREHOUSE": "postgres", "PG_HOST": "127.0.0.1", "PG_PORT": "5432", "PG_USER": "app",
            "PG_PASSWORD": "app_local_pw", "PG_DB": "warehouse"}
STEPS = ("bronze", "silver", "gold")


def run_step(name: str, batch: str, run_id: str, spark_holder: dict):
    if name == "gold":
        from engine.gold import run_gold
        return run_gold(batch, run_id)
    if "spark" not in spark_holder:  # Spark only when bronze/silver are requested (lazy imports)
        from engine.spark import get_spark
        spark_holder["spark"] = get_spark(f"cdt-local-{batch}")
    if name == "bronze":
        from engine.bronze import run_bronze
        return run_bronze(spark_holder["spark"], batch, run_id)
    from engine.silver import run_silver
    return run_silver(spark_holder["spark"], batch, run_id)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", required=True)
    ap.add_argument("--steps", default=",".join(STEPS))
    ap.add_argument("--run-id", default=None)
    a = ap.parse_args(argv)
    steps = [s.strip() for s in a.steps.split(",") if s.strip()]
    bad = [s for s in steps if s not in STEPS]
    if bad:
        ap.error(f"unknown steps {bad}; choose from {list(STEPS)}")
    for k, v in DEFAULTS.items():
        os.environ.setdefault(k, v)
    from engine.runlog import log, new_run_id, step

    raw = Path(os.environ["DATA_ROOT"]) / "raw" / a.batch
    sample = Path(os.environ["PROJECT_DIR"]) / "sample_data" / "raw" / a.batch
    if "bronze" in steps and not os.environ["DATA_ROOT"].startswith("s3") and not raw.exists() and sample.is_dir():
        shutil.copytree(sample, raw)
        log(f"LANDED sample files {sample} -> {raw}")
    run_id = a.run_id or new_run_id()
    log(f"PIPELINE_START run={run_id} batch={a.batch} steps={steps}")
    spark_holder: dict = {}
    try:
        for name in steps:
            with step(run_id, a.batch, name) as result:
                out = run_step(name, a.batch, run_id, spark_holder)
                result["message"] = str(out)[:2000] if out is not None else ""
    except Exception:  # noqa: BLE001 - already logged as ERROR STEP_FAILED by runlog
        log(f"ERROR PIPELINE_FAILED run={run_id} batch={a.batch}")
        return 1
    finally:
        if "spark" in spark_holder:
            spark_holder["spark"].stop()
    log(f"PIPELINE_OK run={run_id} batch={a.batch}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
