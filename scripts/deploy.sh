#!/bin/bash
# Deploys code to an applied stack. Run from the repo root (e.g. in AWS CloudShell) AFTER terraform apply:
#   bash scripts/deploy.sh hack-dev
# 1. package Glue code -> s3://<bucket>/glue/   (jobs pick it up on their next run)
# 2. build + push the dashboard image to ECR     (SKIP_IMAGE=1 to skip, e.g. when only rules/SQL changed)
# 3. copy server scripts -> s3://<bucket>/ops/server/
# 4. SSM: the dashboard server pulls the image and restarts the container (SKIP_SERVER=1 to skip)
# Names follow the Terraform convention <project>-<env>; override with BUCKET / REPO_URL / INSTANCE_ID env vars.
set -euo pipefail
NAME="${1:?usage: bash scripts/deploy.sh <project>-<env>   e.g. hack-dev}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
export AWS_REGION="${AWS_REGION:-ap-south-1}" AWS_DEFAULT_REGION="${AWS_REGION:-ap-south-1}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="${BUCKET:-${NAME}-data-${ACCOUNT}}"
REGISTRY="${ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
REPO_URL="${REPO_URL:-${REGISTRY}/${NAME}-app}"
aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1 || { echo "ERROR bucket $BUCKET not found - terraform apply first?"; exit 1; }

echo "== 1/4 Glue code -> s3://$BUCKET/glue/"
bash scripts/package_glue.sh
aws s3 sync dist/ "s3://$BUCKET/glue/" --delete --only-show-errors
aws s3 ls "s3://$BUCKET/glue/" --recursive | awk '{print "   ", $3, $4}'

if [ "${SKIP_IMAGE:-0}" != 1 ]; then
  echo "== 2/4 dashboard image -> $REPO_URL:latest"
  aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null
  TAG=$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d%H%M%S)
  docker build -f docker/Dockerfile.dashboard -t "$REPO_URL:latest" -t "$REPO_URL:$TAG" .
  docker push --quiet "$REPO_URL:$TAG" && docker push --quiet "$REPO_URL:latest"
else echo "== 2/4 image skipped (SKIP_IMAGE=1)"; fi

echo "== 3/4 server scripts -> s3://$BUCKET/ops/server/"
aws s3 cp scripts/server/ "s3://$BUCKET/ops/server/" --recursive --only-show-errors

if [ "${SKIP_SERVER:-0}" = 1 ]; then echo "== 4/4 server restart skipped (SKIP_SERVER=1)"; exit 0; fi
echo "== 4/4 restart the dashboard on the server"
INSTANCE_ID="${INSTANCE_ID:-$(aws ec2 describe-instances --filters "Name=tag:Name,Values=${NAME}-server" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)}"
[ -n "$INSTANCE_ID" ] && [ "$INSTANCE_ID" != None ] || { echo "ERROR no running ${NAME}-server instance"; exit 1; }
PARAMS=$(python3 -c 'import json,sys; b,n=sys.argv[1:]; print(json.dumps({"commands": [
  "set -e", "mkdir -p /opt/app/ops",
  f"aws s3 cp s3://{b}/ops/server/ /opt/app/ops/ --recursive --quiet",
  f"bash /opt/app/ops/deploy.sh {n}"], "executionTimeout": ["900"]}))' "$BUCKET" "$NAME")
CMD=$(aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --comment "deploy dashboard" --parameters "$PARAMS" --query Command.CommandId --output text)
for _ in $(seq 1 120); do
  sleep 5
  S=$(aws ssm get-command-invocation --command-id "$CMD" --instance-id "$INSTANCE_ID" --query Status --output text 2>/dev/null || echo Pending)
  case $S in Success|Failed|Cancelled|TimedOut) break;; esac
done
aws ssm get-command-invocation --command-id "$CMD" --instance-id "$INSTANCE_ID" --query '[StandardOutputContent,StandardErrorContent]' --output text | tail -20
echo "server deploy: $S"
[ "$S" = Success ]
