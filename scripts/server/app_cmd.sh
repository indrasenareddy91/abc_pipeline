#!/bin/bash
# Runs ON the dashboard server: any command inside the dashboard image with the app config
# (Redshift Data API settings, bucket...). Run deploy.sh once first (it writes /opt/app/app.env).
# Example: bash /opt/app/ops/app_cmd.sh python -c "from engine.warehouse import *; print('ok')"
set -euo pipefail
set -a; source /opt/app/app.env; set +a
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "${IMAGE_URL%%/*}" >/dev/null 2>&1
docker pull -q "${IMAGE_URL}:latest" >/dev/null   # always the newest image
docker run --rm --env-file /opt/app/app.env "${IMAGE_URL}:latest" "$@"
