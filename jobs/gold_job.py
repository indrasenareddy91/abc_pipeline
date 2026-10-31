"""Glue / local entrypoint: GOLD for one batch (load warehouse, SCD2 + fact MERGE, KPIs, checks).

Glue:  --batch_id day1 --run_id <id> [--data_root s3://bucket] [--project_dir /path]
       [--warehouse redshift --redshift_workgroup wg --redshift_db dev --redshift_secret_arn arn --aws_region r]
       Glue does not turn job arguments into environment variables, so the warehouse_* arguments are exported as
       WAREHOUSE / REDSHIFT_WORKGROUP / REDSHIFT_DB / REDSHIFT_SECRET_ARN / AWS_REGION here (env wins if already set).
Local: python jobs/gold_job.py --batch_id day1 [--run_id r1]   (DATA_ROOT / PROJECT_DIR / PG_* from env)"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # local: repo root on sys.path

from engine import config  # noqa: E402

ENV_ARGS = {"warehouse": "WAREHOUSE", "redshift_workgroup": "REDSHIFT_WORKGROUP", "redshift_db": "REDSHIFT_DB",
            "redshift_secret_arn": "REDSHIFT_SECRET_ARN", "aws_region": "AWS_REGION"}


def main() -> None:
    args = config.job_args(optional=("JOB_NAME", "run_id", "data_root", "project_dir", *ENV_ARGS))
    for arg, env in ENV_ARGS.items():
        if args.get(arg) and not os.environ.get(env):
            os.environ[env] = args[arg]
    from engine.gold import run_gold
    from engine.runlog import step

    with step(args["run_id"], args["batch_id"], "gold") as result:
        summary = run_gold(args["batch_id"], args["run_id"])
        result["message"] = f"{summary['checks_passed']} checks passed; kpi tables: {len(summary['kpi_tables'])}"


if __name__ == "__main__":
    main()
