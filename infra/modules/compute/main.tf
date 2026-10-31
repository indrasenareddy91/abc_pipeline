# Dashboard server (EC2): runs ONLY the Streamlit dashboard as a Docker container (the pipeline runs in Glue).
# - No SSH key, no SSH port: managed via AWS SSM Session Manager.
# - IAM role with least privilege: READ its own bucket, query its own Redshift workgroup through the Data API
#   (+ read the warehouse secret), start/watch its own pipeline workflow ("Run now" button), its own config
#   parameter, its own log group, pull its own image. Nothing else.
# - Settings live in an encrypted SSM parameter (written by Terraform), not baked into the server or image.

variable "name" { type = string }
variable "region" { type = string }
variable "instance_type" { type = string }
variable "root_volume_gb" { type = number }
variable "subnet_id" { type = string }
variable "security_group_id" { type = string }
variable "bucket_name" { type = string }
variable "bucket_arn" { type = string }
variable "ecr_repository_arn" { type = string }
variable "ecr_repository_url" { type = string }
variable "app_log_group" { type = string }
variable "app_log_group_arn" { type = string }
variable "pipeline_log_group" { type = string }
variable "dashboard_port" { type = number }
variable "dashboard_password" {
  type      = string
  default   = ""
  sensitive = true
}
variable "state_machine_arn" { type = string }
variable "redshift_workgroup" { type = string }
variable "redshift_workgroup_arn" { type = string }
variable "redshift_db" { type = string }
variable "redshift_secret_arn" { type = string }
# optional RDS (enable_database = true)
variable "db_secret_arn" {
  type    = string
  default = ""
}
variable "db_host" {
  type    = string
  default = ""
}

data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_caller_identity" "me" {}

# ---------------- IAM ----------------
resource "aws_iam_role" "server" {
  name = "${var.name}-server-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.server.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

locals {
  # arn:aws:states:<r>:<acct>:stateMachine:<n>  ->  arn:aws:states:<r>:<acct>:execution:<n>:*
  executions_arn = "${replace(var.state_machine_arn, ":stateMachine:", ":execution:")}:*"
  base_statements = [
    { Sid = "ListOwnBucket", Effect = "Allow", Action = ["s3:ListBucket"], Resource = [var.bucket_arn] },
    { Sid = "ReadOwnBucket", Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${var.bucket_arn}/*"] },
    {
      Sid      = "ReadOwnConfig"
      Effect   = "Allow"
      Action   = ["ssm:GetParameter", "ssm:GetParameters"]
      Resource = ["arn:aws:ssm:${var.region}:${data.aws_caller_identity.me.account_id}:parameter/${var.name}/*"]
    },
    { Sid = "WriteOwnLogs", Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"], Resource = ["${var.app_log_group_arn}:*"] },
    { Sid = "EcrLogin", Effect = "Allow", Action = ["ecr:GetAuthorizationToken"], Resource = ["*"] },
    {
      Sid      = "PullOwnImage"
      Effect   = "Allow"
      Action   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"]
      Resource = [var.ecr_repository_arn]
    },
    { Sid = "QueryWarehouse", Effect = "Allow", Action = ["redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement"], Resource = [var.redshift_workgroup_arn] },
    {
      Sid      = "ReadQueryResults" # only "*" is supported; limited to statements this role ran
      Effect   = "Allow"
      Action   = ["redshift-data:DescribeStatement", "redshift-data:GetStatementResult", "redshift-data:GetStatementResultV2", "redshift-data:CancelStatement"]
      Resource = ["*"]
    },
    { Sid = "ReadWarehouseSecret", Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = [var.redshift_secret_arn] },
    { Sid = "RunNowButton", Effect = "Allow", Action = ["states:StartExecution", "states:ListExecutions"], Resource = [var.state_machine_arn] },
    { Sid = "WatchRuns", Effect = "Allow", Action = ["states:DescribeExecution"], Resource = [local.executions_arn] },
  ]
  db_statements = var.db_secret_arn == "" ? [] : [
    { Sid = "ReadDbSecret", Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = [var.db_secret_arn] },
  ]
}

resource "aws_iam_role_policy" "least_privilege" {
  name = "${var.name}-least-privilege"
  role = aws_iam_role.server.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = concat(local.base_statements, local.db_statements)
  })
}

resource "aws_iam_instance_profile" "server" {
  name = "${var.name}-server-profile"
  role = aws_iam_role.server.name
}

# ---------------- App config (env file for the dashboard container) ----------------
resource "aws_ssm_parameter" "app_config" {
  name = "/${var.name}/app-config"
  type = "SecureString" # encrypted with the free AWS-managed key (holds the dashboard password)
  value = join("\n", compact([
    "RUN_MODE=aws",
    "AWS_REGION=${var.region}",
    "AWS_DEFAULT_REGION=${var.region}",
    "DATA_ROOT=s3://${var.bucket_name}",
    "WAREHOUSE=redshift",
    "REDSHIFT_WORKGROUP=${var.redshift_workgroup}",
    "REDSHIFT_DB=${var.redshift_db}",
    "REDSHIFT_SECRET_ARN=${var.redshift_secret_arn}",
    "STATE_MACHINE_ARN=${var.state_machine_arn}",
    "DASHBOARD_PASSWORD=${var.dashboard_password}",
    "APP_LOG_GROUP=${var.app_log_group}",
    "PIPELINE_LOG_GROUP=${var.pipeline_log_group}",
    "IMAGE_URL=${var.ecr_repository_url}",
    "DASHBOARD_PORT=${var.dashboard_port}",
    var.db_host != "" ? "DB_HOST=${var.db_host}" : "",
    var.db_secret_arn != "" ? "DB_SECRET_ARN=${var.db_secret_arn}" : "",
  ]))
}

# ---------------- Server ----------------
resource "aws_instance" "server" {
  ami                         = data.aws_ssm_parameter.al2023_ami.value
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  iam_instance_profile        = aws_iam_instance_profile.server.name
  associate_public_ip_address = true
  ebs_optimized               = true
  monitoring                  = false # detailed monitoring costs extra; basic 5-min metrics are enough
  user_data                   = file("${path.module}/user_data.sh")
  user_data_replace_on_change = true # bootstrap changes -> fresh server, so the script really re-runs

  metadata_options {
    http_tokens                 = "required" # IMDSv2 only
    http_put_response_hop_limit = 2          # lets the Docker container use the instance role
  }

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }

  credit_specification {
    cpu_credits = "standard" # never bill for extra CPU ("unlimited" mode can)
  }

  lifecycle {
    ignore_changes = [ami] # don't rebuild the server just because a newer AMI exists
  }

  tags = { Name = "${var.name}-server" }
}

output "instance_id" { value = aws_instance.server.id }
output "instance_arn" { value = aws_instance.server.arn }
output "public_ip" { value = aws_instance.server.public_ip }
output "role_arn" { value = aws_iam_role.server.arn }
output "config_parameter" { value = aws_ssm_parameter.app_config.name }
