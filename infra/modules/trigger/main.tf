# Orchestration:
#   S3 raw/<batch>/ ObjectCreated -> Lambda (are all required files there?) -> Step Functions
#   Step Functions: Run bronze -> Run silver -> Run gold (Glue jobs, .sync = waits for each to finish)
#   any failure -> email via the alerts topic -> Fail
# Execution input: {"batch": "day1"}. Each job gets --batch_id <batch> and --run_id <execution name>.
# Lambda + S3 events + Step Functions at hackathon volume cost ~ $0.

variable "name" { type = string }
variable "bucket_name" { type = string }
variable "bucket_arn" { type = string }
variable "required_files" { type = list(string) }
variable "enable_file_trigger" { type = bool }
variable "glue_jobs" { type = list(string) } # ordered: bronze, silver, gold
variable "glue_job_names" { type = map(string) }
variable "glue_job_arns" { type = list(string) }
variable "alerts_topic_arn" { type = string }
variable "log_retention_days" { type = number }

data "aws_caller_identity" "current" {}

# ---------------- Step Functions ----------------
locals {
  sfn_states = merge([for i, step in var.glue_jobs : {
    "Run ${step}" = {
      Type     = "Task"
      Resource = "arn:aws:states:::glue:startJobRun.sync"
      Parameters = {
        JobName = var.glue_job_names[step]
        Arguments = {
          "--batch_id.$" = "$.batch"
          "--run_id.$"   = "$$.Execution.Name"
        }
      }
      ResultSelector = { "JobRunId.$" = "$.Id", "State.$" = "$.JobRunState", "ExecutionTime.$" = "$.ExecutionTime" }
      ResultPath     = "$.${step}"
      Retry = [{
        ErrorEquals     = ["Glue.ConcurrentRunsExceededException", "Glue.ThrottlingException"]
        IntervalSeconds = 30
        MaxAttempts     = 20
        BackoffRate     = 1.5
        MaxDelaySeconds = 120
      }]
      Catch = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "Notify failure" }]
      Next  = i + 1 < length(var.glue_jobs) ? "Run ${var.glue_jobs[i + 1]}" : "Pipeline OK"
    }
  }]...)
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${var.name}-pipeline"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "sfn" {
  name = "${var.name}-pipeline-workflow"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "sfn" {
  name = "least-privilege"
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Sid = "RunOwnGlueJobs", Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"], Resource = var.glue_job_arns },
      { Sid = "FailureEmail", Effect = "Allow", Action = "sns:Publish", Resource = var.alerts_topic_arn },
      {
        Sid    = "ExecutionLogs" # CloudWatch log delivery APIs only support "*"
        Effect = "Allow"
        Action = ["logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery", "logs:ListLogDeliveries",
        "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.name}-pipeline"
  role_arn = aws_iam_role.sfn.arn
  definition = jsonencode({
    Comment = "Data pipeline: Glue ${join(" -> ", var.glue_jobs)}. Input: {\"batch\": \"day1\"}"
    StartAt = "Run ${var.glue_jobs[0]}"
    States = merge(local.sfn_states, {
      "Pipeline OK" = { Type = "Succeed" }
      "Notify failure" = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = var.alerts_topic_arn
          Subject     = "${var.name}: pipeline workflow failed"
          "Message.$" = "States.Format('Pipeline failed for batch {}. Error: {}. Cause: {}. Open Step Functions (${var.name}-pipeline) and the /aws-glue/jobs/${var.name}/output logs.', $.batch, $.error.Error, $.error.Cause)"
        }
        ResultPath = null
        Next       = "Pipeline failed"
      }
      "Pipeline failed" = { Type = "Fail", Error = "PipelineFailed", Cause = "A Glue job failed; see the failure email / execution history" }
    })
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true # input is only {"batch": ...}; no data or secrets pass through the workflow
    level                  = "ERROR"
  }

  depends_on = [aws_iam_role_policy.sfn]
}

# ---------------- Lambda (files-arrived) ----------------
data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/lambda.py"
  output_path = "${path.module}/.build/lambda.zip"
}

resource "aws_cloudwatch_log_group" "lambda" {
  count             = var.enable_file_trigger ? 1 : 0
  name              = "/aws/lambda/${var.name}-files-arrived"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "lambda" {
  count = var.enable_file_trigger ? 1 : 0
  name  = "${var.name}-files-arrived"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  count = var.enable_file_trigger ? 1 : 0
  name  = "least-privilege"
  role  = aws_iam_role.lambda[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = "s3:ListBucket", Resource = var.bucket_arn, Condition = { StringLike = { "s3:prefix" = ["raw/*"] } } },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.lambda[0].arn}:*" },
      { Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.pipeline.arn },
    ]
  })
}

resource "aws_lambda_function" "files_arrived" {
  count            = var.enable_file_trigger ? 1 : 0
  function_name    = "${var.name}-files-arrived"
  role             = aws_iam_role.lambda[0].arn
  runtime          = "python3.12"
  handler          = "lambda.handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 30
  memory_size      = 128
  # one invocation at a time per burst is not needed: the execution name dedupes identical file sets
  environment {
    variables = {
      BUCKET            = var.bucket_name
      REQUIRED_FILES    = join(",", var.required_files)
      STATE_MACHINE_ARN = aws_sfn_state_machine.pipeline.arn
    }
  }
  depends_on = [aws_cloudwatch_log_group.lambda]
}

resource "aws_lambda_permission" "s3" {
  count          = var.enable_file_trigger ? 1 : 0
  statement_id   = "AllowS3"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.files_arrived[0].function_name
  principal      = "s3.amazonaws.com"
  source_arn     = var.bucket_arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "raw" {
  count  = var.enable_file_trigger ? 1 : 0
  bucket = var.bucket_name
  lambda_function {
    lambda_function_arn = aws_lambda_function.files_arrived[0].arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }
  depends_on = [aws_lambda_permission.s3]
}

output "state_machine_arn" { value = aws_sfn_state_machine.pipeline.arn }
output "state_machine_name" { value = aws_sfn_state_machine.pipeline.name }
output "lambda_name" { value = var.enable_file_trigger ? aws_lambda_function.files_arrived[0].function_name : null }
