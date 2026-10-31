#!/bin/bash
# Packages the code Glue runs into dist/ (run from anywhere; paths are relative to the repo root):
#   dist/engine.zip    engine/  package           (--extra-py-files -> importable as "engine")
#   dist/project.zip   project/config, rules, sql (no sample_data; top-level folder "project/")
#   dist/jobs/*.py     Glue entrypoints           (script_location s3://<bucket>/glue/jobs/<step>_job.py)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
for d in engine jobs project/config; do [ -d "$d" ] || { echo "ERROR missing $d/ (run from the repo)"; exit 1; }; done
rm -rf dist && mkdir -p dist/jobs

python3 - <<'PY'
import os, zipfile
def pack(zip_path, roots):
    n = 0
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root in roots:
            if not os.path.isdir(root):
                continue
            for d, dirs, files in os.walk(root):
                dirs[:] = sorted(x for x in dirs if x not in ("__pycache__", ".pytest_cache", "sample_data"))
                for f in sorted(files):
                    if f.endswith((".pyc", ".pyo")) or f == ".DS_Store":
                        continue
                    p = os.path.join(d, f)
                    info = zipfile.ZipInfo(p, date_time=(2020, 1, 1, 0, 0, 0))  # fixed time = same input, same zip
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.external_attr = 0o644 << 16
                    z.writestr(info, open(p, "rb").read())
                    n += 1
    print(f"{zip_path}: {n} files")
pack("dist/engine.zip", ["engine"])
pack("dist/project.zip", ["project/config", "project/rules", "project/sql"])
PY

cp jobs/*_job.py dist/jobs/
ls dist/jobs
