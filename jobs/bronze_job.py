"""Glue / local entrypoint: BRONZE for one batch.

Glue:  --batch_id day1 --run_id <id> [--data_root s3://bucket] [--project_dir /path]
       (engine.zip + project.zip shipped with --extra-py-files)
Local: python jobs/bronze_job.py --batch_id day1 [--run_id r1]   (DATA_ROOT / PROJECT_DIR from env)"""
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # local: repo root on sys.path

from engine import config  # noqa: E402


def main() -> None:
    args = config.job_args()
    batch_id, run_id = args["batch_id"], args["run_id"]
    try:
        from engine.bronze import run_bronze
        from engine.runlog import step
        from engine.spark import get_spark
        with step(run_id, batch_id, "bronze"):  # pipeline_runs row (shown in the dashboard)
            run_bronze(get_spark(args.get("JOB_NAME", "cdt-bronze")), batch_id, run_id)
    except Exception as exc:
        print(f"ERROR step=bronze batch={batch_id} run_id={run_id} {type(exc).__name__}: {exc}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
