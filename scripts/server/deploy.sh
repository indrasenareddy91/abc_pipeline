#!/bin/bash
# Runs ON the dashboard server (via SSM, as root). Pulls the latest dashboard image and (re)starts it.
# Usage: bash deploy.sh <project>-<env>   e.g. hack-dev
set -euo pipefail
NAME="${1:?usage: deploy.sh <project>-<env>}"

# Settings (Redshift workgroup/secret ARN, state machine, dashboard password...) come from the encrypted
# SSM parameter written by Terraform; the file is readable by root only.
umask 077
mkdir -p /opt/app
aws ssm get-parameter --name "/${NAME}/app-config" --with-decryption --query Parameter.Value --output text > /opt/app/app.env
set -a; source /opt/app/app.env; set +a

REGISTRY="${IMAGE_URL%%/*}"
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null
docker pull -q "${IMAGE_URL}:latest"

docker rm -f dashboard 2>/dev/null || true
docker run -d --name dashboard --restart unless-stopped \
  --env-file /opt/app/app.env \
  -p "${DASHBOARD_PORT}:8501" \
  --log-driver awslogs --log-opt awslogs-region="$AWS_REGION" --log-opt awslogs-group="$APP_LOG_GROUP" \
  "${IMAGE_URL}:latest"
docker image prune -f >/dev/null 2>&1 || true

for _ in $(seq 1 20); do
  curl -fsS "http://localhost:${DASHBOARD_PORT}/_stcore/health" >/dev/null 2>&1 && { echo "dashboard healthy on port ${DASHBOARD_PORT}"; exit 0; }
  sleep 3
done
echo "ERROR dashboard not healthy; last logs:"; docker logs --tail 30 dashboard 2>&1 || true
exit 1
