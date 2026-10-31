# Lets GitHub Actions deploy WITHOUT stored AWS keys (OIDC) - same steps as scripts/deploy.sh:
#   - upload Glue code (s3://<bucket>/glue/) and server scripts (ops/server/)
#   - push the dashboard image to ECR
#   - tell the server (via SSM) to pull the new image and restart
# Only workflows on the main branch of ONE repo can use this role.

variable "name" { type = string }
variable "github_repo" { type = string } # "owner/repo"
variable "create_oidc_provider" { type = bool }
variable "ecr_repository_arn" { type = string }
variable "instance_arn" { type = string }
variable "bucket_arn" { type = string }

data "aws_caller_identity" "me" {}

resource "aws_iam_openid_connect_provider" "github" {
  count           = var.create_oidc_provider ? 1 : 0
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

locals {
  oidc_arn = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:aws:iam::${data.aws_caller_identity.me.account_id}:oidc-provider/token.actions.githubusercontent.com"
}

resource "aws_iam_role" "deploy" {
  name = "${var.name}-github-deploy"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = local.oidc_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repo}:ref:refs/heads/main"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "deploy" {
  name = "${var.name}-github-deploy"
  role = aws_iam_role.deploy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "EcrLogin"
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = ["*"]
      },
      {
        Sid    = "PushOwnImage"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"
        ]
        Resource = [var.ecr_repository_arn]
      },
      {
        Sid      = "ListOwnBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.bucket_arn]
      },
      {
        Sid      = "UploadCodeAndServerScripts"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${var.bucket_arn}/glue/*", "${var.bucket_arn}/ops/server/*"]
      },
      {
        Sid      = "RunDeployCommandOnOwnServer"
        Effect   = "Allow"
        Action   = ["ssm:SendCommand"]
        Resource = [var.instance_arn, "arn:aws:ssm:*::document/AWS-RunShellScript"]
      },
      {
        Sid      = "ReadCommandResult"
        Effect   = "Allow"
        Action   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations"]
        Resource = ["*"]
      },
    ]
  })
}

output "role_arn" { value = aws_iam_role.deploy.arn }
