"""Files-arrived trigger. S3 says "a file landed in raw/<batch>/..." -> if every required file for that batch
folder is there, start the pipeline workflow (Step Functions: Glue bronze -> silver -> gold) with {"batch": <batch>}.
Same set of files twice -> started once (execution name = batch + fingerprint of the files)."""
import datetime
import fnmatch
import hashlib
import json
import os
import re
import urllib.parse

import boto3

s3 = boto3.client("s3")
sfn = boto3.client("stepfunctions")
BUCKET = os.environ["BUCKET"]
SFN_ARN = os.environ["STATE_MACHINE_ARN"]
REQUIRED = [p.strip().lower() for p in os.environ.get("REQUIRED_FILES", "").split(",") if p.strip()]


def handler(event, context):
    keys = [urllib.parse.unquote_plus(r["s3"]["object"]["key"]) for r in event.get("Records", [])]
    batches = {k.split("/")[1] if k.count("/") >= 2 else "" for k in keys if k.startswith("raw/") and not k.endswith("/")}
    started = []
    for batch in sorted(batches):
        prefix = f"raw/{batch}/" if batch else "raw/"
        objs = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
            objs += [o for o in page.get("Contents", []) if o["Key"][len(prefix):] and "/" not in o["Key"][len(prefix):]]
        names = [o["Key"][len(prefix):].lower() for o in objs]
        missing = [p for p in REQUIRED if not any(fnmatch.fnmatch(n, p) for n in names)]
        if missing:
            print(json.dumps({"batch": batch or "(raw/)", "waiting_for": missing}))
            continue
        batch_id = batch or datetime.date.today().isoformat()
        fp = hashlib.sha256("".join(sorted(o["Key"] + o["ETag"] for o in objs)).encode()).hexdigest()[:16]
        run_name = re.sub(r"[^A-Za-z0-9_-]", "_", f"{batch_id}-{fp}")[:80]
        try:
            sfn.start_execution(stateMachineArn=SFN_ARN, name=run_name, input=json.dumps({"batch": batch_id}))
        except sfn.exceptions.ExecutionAlreadyExists:
            print(json.dumps({"batch": batch_id, "skipped": "already started for these exact files"}))
            continue
        started.append(batch_id)
        print(json.dumps({"batch": batch_id, "started": run_name, "files": names}))
    return {"started": started}
