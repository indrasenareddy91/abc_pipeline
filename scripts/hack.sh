#!/bin/bash
# One command per hackathon step. Run from the repo root.
#   Laptop (Git Bash / Linux, Docker Desktop running):
#     bash scripts/hack.sh local day1 day2     postgres + dashboard up, pipeline runs each batch  -> http://127.0.0.1:8501
#     bash scripts/hack.sh stop                stop the local containers (data kept)
#     bash scripts/hack.sh reset               wipe local warehouse + data/ (fresh start)
#   AWS CloudShell (after: unzip cdt.zip or git clone):
#     bash scripts/hack.sh cloud-up            terraform init + apply (once; ~15 min) -> stack hack-dev
#     bash scripts/hack.sh deploy              push Glue code + dashboard image (after every code change)
#     bash scripts/hack.sh deploy-fast         Glue code only (rules/SQL/config changed) ~30 s
#     bash scripts/hack.sh run day1 day2       upload project/sample_data/raw/<batch> -> trigger -> Glue -> Redshift, checks
#     bash scripts/hack.sh upload day1 <dir>   upload your own files for a batch (+ _READY -> pipeline starts)
#     bash scripts/hack.sh status              last Glue runs + workflow runs
#     bash scripts/hack.sh url                 dashboard URL (password: terraform output -raw dashboard_password)
#     bash scripts/hack.sh cloud-down          terraform destroy (everything, incl. data)
# NAME defaults to hack-dev (project_name-environment in terraform.tfvars); override: NAME=x-dev bash scripts/hack.sh ...
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
NAME="${NAME:-hack-dev}"
export AWS_REGION="${AWS_REGION:-ap-south-1}" AWS_DEFAULT_REGION="${AWS_REGION:-ap-south-1}"
CMD="${1:-help}"; shift || true
DC="docker compose -f docker/docker-compose.yml"
log() { echo "== $*"; }

case "$CMD" in
local)
  [ -f docker/.env ] || cp docker/.env.example docker/.env
  P="${PROJECT:-project}"
  export PROJECT_PATH="../$P"   # relative to docker/ (works in Git Bash on Windows)
  [ -f "$P/config/pipeline.yaml" ] || { echo "$P/config/pipeline.yaml missing"; exit 1; }
  BATCHES="${*:-$(ls "$P/sample_data/raw" 2>/dev/null | sort | tr '\n' ' ')}"
  [ -n "$BATCHES" ] || { echo "no batches: put files in $P/sample_data/raw/<batch>/"; exit 1; }
  mkdir -p data/raw
  for b in $BATCHES; do
    [ -d "data/raw/$b" ] || { [ -d "$P/sample_data/raw/$b" ] && cp -r "$P/sample_data/raw/$b" data/raw/; }
  done
  chmod -R a+rwX data 2>/dev/null || true
  log "build + start postgres and dashboard"
  $DC up -d --build postgres dashboard
  $DC --profile run build pipeline
  for b in $BATCHES; do log "pipeline batch $b"; $DC --profile run run --rm pipeline --batch "$b"; done
  log "done. dashboard: http://127.0.0.1:8501   reports: data/reports/latest/" ;;
stop)  $DC stop ;;
reset) $DC down -v; rm -rf data; log "local warehouse and data/ wiped" ;;
test)
  python -m pytest -q tests/test_engine_v2_features.py tests/test_reports_unit.py tests/test_dashboard_*.py "$@" ;;

cloud-up)
  cd infra/stack
  [ -f terraform.tfvars ] || cp terraform.tfvars.example terraform.tfvars
  [ -f backend.hcl ] || cp backend.hcl.example backend.hcl
  export TF_PLUGIN_CACHE_DIR=/tmp/tfcache; mkdir -p "$TF_PLUGIN_CACHE_DIR"
  terraform init -input=false -backend-config=backend.hcl -no-color > /tmp/tf-init.log 2>&1 || { tail -20 /tmp/tf-init.log; exit 1; }
  log "terraform apply (~15 min; log: /tmp/tf-apply.log)"
  terraform apply -input=false -auto-approve -no-color > /tmp/tf-apply.log 2>&1 || { grep -A8 Error /tmp/tf-apply.log | head -40; exit 1; }
  grep -E "Apply complete" /tmp/tf-apply.log
  cd "$ROOT"; log "next: bash scripts/hack.sh deploy" ;;
cloud-down)
  cd infra/stack; terraform destroy -input=false -no-color ;;
deploy)      bash scripts/deploy.sh "$NAME" ;;
deploy-fast) SKIP_IMAGE=1 SKIP_SERVER=1 bash scripts/deploy.sh "$NAME" ;;
run)         bash scripts/e2e_aws.sh "$NAME" "$@" ;;
upload)
  B="${1:?usage: hack.sh upload <batch> <local dir>}"; D="${2:?usage: hack.sh upload <batch> <local dir>}"
  BUCKET="$NAME-data-$(aws sts get-caller-identity --query Account --output text)"
  aws s3 cp "$D" "s3://$BUCKET/raw/$B/" --recursive --exclude "_READY" --only-show-errors
  aws s3 cp - "s3://$BUCKET/raw/$B/_READY" --only-show-errors <<< "$(date -u +%FT%TZ)"
  log "uploaded $D -> raw/$B/ + _READY (pipeline starts; watch: bash scripts/hack.sh status)" ;;
status)
  for j in bronze silver gold; do
    aws glue get-job-runs --job-name "$NAME-$j" --max-items 1 \
      --query "JobRuns[0].[JobName,JobRunState,ExecutionTime,ErrorMessage]" --output text | head -1
  done
  SM=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='$NAME-pipeline'].stateMachineArn" --output text)
  aws stepfunctions list-executions --state-machine-arn "$SM" --max-results 5 --query "executions[].[name,status]" --output text ;;
url) cd infra/stack && terraform output -raw dashboard_url && echo ;;
*) sed -n 2,19p "$0" ;;
esac
