# Glue Spark jobs: bronze -> silver -> gold (one job per medallion step, run in order by Step Functions).
# Code is NOT baked in here: scripts/deploy.sh uploads to s3://<bucket>/glue/
#   jobs/<name>_job.py   entrypoints     engine.zip   the engine package     project.zip   config/rules/sql
# Cost: G.1X x 2 workers = 2 DPU, ~$0.44 per DPU-hour, billed per second with a 1-minute minimum.

variable "name" { type = string }
variable "bucket_name" { type = string }
variable "bucket_arn" { type = string }
variable "log_group_prefix" { type = string }
variable "log_group_arns" { type = list(string) }
variable "redshift_workgroup" { type = string }
variable "redshift_workgroup_arn" { type = string }
variable "redshift_db" { type = string }
variable "redshift_secret_arn" { type = string }
variable "worker_type" { type = string }
variable "number_of_workers" { type = number }
variable "timeout_minutes" { type = number }
variable "jobs" { type = list(string) }

# ---------------- IAM ----------------
resource "aws_iam_role" "glue" {
  name = "${var.name}-glue"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

# Glue's own basics (its temp bucket aws-glue-*, CloudWatch metrics, /aws-glue/* logs).
resource "aws_iam_role_policy_attachment" "service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

locals {
  # Also allows "<prefix>_$folder$": the empty marker object Glue's S3 filesystem writes when it overwrites a folder
  write_prefixes = ["bronze", "silver", "silver_changes", "quarantine", "ops", "reports"]
}

resource "aws_iam_role_policy" "glue" {
  name = "least-privilege"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Sid = "ListBucket", Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"], Resource = var.bucket_arn },
      { Sid = "ReadRawAndCode", Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${var.bucket_arn}/raw/*", "${var.bucket_arn}/glue/*"] },
      {
        Sid      = "ReadWriteLayers" # Spark overwrite of a batch partition needs Delete
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = flatten([for p in local.write_prefixes : ["${var.bucket_arn}/${p}/*", "${var.bucket_arn}/${p}_$folder$"]])
      },
      {
        Sid      = "NeverTouchServerScripts" # ops/server/ holds scripts the dashboard server runs as root
        Effect   = "Deny"
        Action   = ["s3:PutObject", "s3:DeleteObject"]
        Resource = ["${var.bucket_arn}/ops/server/*"]
      },
      { Sid = "RedshiftDataRun", Effect = "Allow", Action = ["redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement"], Resource = var.redshift_workgroup_arn },
      {
        Sid      = "RedshiftDataResults" # these actions only support "*"; they are limited to statements this role ran
        Effect   = "Allow"
        Action   = ["redshift-data:DescribeStatement", "redshift-data:GetStatementResult", "redshift-data:GetStatementResultV2", "redshift-data:CancelStatement", "redshift-data:ListStatements"]
        Resource = "*"
      },
      { Sid = "ReadWarehouseSecret", Effect = "Allow", Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"], Resource = var.redshift_secret_arn },
      {
        Sid      = "WriteOwnJobLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
        Resource = [for arn in var.log_group_arns : "${arn}:*"]
      },
    ]
  })
}

# ---------------- Jobs ----------------
# Argument names below are the contract with jobs/*.py: the job copies the UPPER-CASE ones into os.environ.
resource "aws_glue_job" "step" {
  for_each          = toset(var.jobs)
  name              = "${var.name}-${each.value}"
  description       = "${each.value} step of the data pipeline (engine.${each.value})"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.timeout_minutes
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_name}/glue/jobs/${each.value}_job.py"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--extra-py-files"                   = "s3://${var.bucket_name}/glue/engine.zip,s3://${var.bucket_name}/glue/project.zip"
    "--additional-python-modules"        = "PyYAML==6.0.2"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--custom-logGroup-prefix"           = var.log_group_prefix
    "--data_root"                        = "s3://${var.bucket_name}"
    "--RUN_MODE"                         = "aws"
    "--WAREHOUSE"                        = "redshift"
    "--REDSHIFT_WORKGROUP"               = var.redshift_workgroup
    "--REDSHIFT_DB"                      = var.redshift_db
    "--REDSHIFT_SECRET_ARN"              = var.redshift_secret_arn
  }
}

output "role_arn" { value = aws_iam_role.glue.arn }
output "job_names" { value = { for k, j in aws_glue_job.step : k => j.name } }
output "job_arns" { value = [for j in aws_glue_job.step : j.arn] }
