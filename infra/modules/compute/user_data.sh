#!/bin/bash
# Runs once when the server is first created (Amazon Linux 2023).
# Installs only what the dashboard needs: Docker, jq, AWS CLI. The app itself arrives later as an image
# (scripts/deploy.sh -> ECR -> SSM runs /opt/app/ops/deploy.sh).
set -euo pipefail

dnf -y update
dnf -y install docker jq awscli-2

systemctl enable --now docker
usermod -aG docker ec2-user

mkdir -p /opt/app/ops
chmod 700 /opt/app
echo "bootstrap complete" > /opt/app/BOOTSTRAP_DONE
