# Optional scheduled run (EventBridge Scheduler), e.g. every day at 06:00 IST.
# Starts the pipeline workflow for a fixed batch folder raw/<batch>/ (input {"batch": "<batch>"}).

variable "name" { type = string }
variable "expression" { type = string }
variable "state_machine_arn" { type = string }
variable "batch" { type = string }

resource "aws_iam_role" "scheduler" {
  name = "${var.name}-scheduler"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "scheduler.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "start-pipeline"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["states:StartExecution"], Resource = [var.state_machine_arn] }]
  })
}

resource "aws_scheduler_schedule" "pipeline" {
  name                         = "${var.name}-pipeline"
  schedule_expression          = var.expression
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = var.state_machine_arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ batch = var.batch })
    retry_policy {
      maximum_retry_attempts = 2
    }
  }
}

output "schedule_name" { value = aws_scheduler_schedule.pipeline.name }
