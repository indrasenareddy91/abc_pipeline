# Logs + alerts:
#   - log groups for the Glue jobs (output/error, via --custom-logGroup-prefix) and the dashboard
#   - email alert when a job or the dashboard logs ERROR or QUARANTINE_ALERT
#   - monthly budget alert (80% actual, 100% forecast)

variable "name" { type = string }
variable "alert_email" { type = string }
variable "budget_usd" { type = number }
variable "log_retention_days" { type = number }

# Glue 5.0 writes job stdout/stderr (our STEP_OK / ERROR / QUARANTINE_ALERT lines and Python tracebacks)
# to <prefix>/output and Spark/system logs to <prefix>/error. We give every project its own prefix
# (job argument --custom-logGroup-prefix) instead of the shared /aws-glue/jobs/output, so the groups can be
# created here with a retention period and alarms never mix projects.
locals {
  glue_log_prefix = "/aws-glue/jobs/${var.name}"
}

resource "aws_cloudwatch_log_group" "glue_output" {
  name              = "${local.glue_log_prefix}/output"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "glue_error" {
  name              = "${local.glue_log_prefix}/error"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "app" {
  name              = "/${var.name}/app"
  retention_in_days = var.log_retention_days
}

resource "aws_sns_topic" "alerts" {
  name = "${var.name}-alerts"
}

# AWS sends a confirmation email: click the link once or alerts won't arrive.
resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Watched groups: the Glue job output (our own log lines + tracebacks) and the dashboard.
# The Glue "error" group is Spark's own log4j stream; it contains harmless ERROR lines in healthy runs,
# so it is kept for debugging but not alarmed on. A failed job is still emailed by Step Functions.
locals {
  alerts = {
    errors     = { pattern = "ERROR", metric = "PipelineErrors", text = "Pipeline logged an ERROR (step failed or data checks failed)" }
    quarantine = { pattern = "QUARANTINE_ALERT", metric = "QuarantineAlerts", text = "Too many rows rejected to quarantine - review them in the dashboard" }
  }
  watched_groups = {
    glue = aws_cloudwatch_log_group.glue_output.name
    app  = aws_cloudwatch_log_group.app.name
  }
  filters = { for pair in setproduct(keys(local.alerts), keys(local.watched_groups)) : "${pair[0]}-${pair[1]}" => {
    alert = pair[0]
    group = local.watched_groups[pair[1]]
  } }
}

resource "aws_cloudwatch_log_metric_filter" "alerts" {
  for_each       = local.filters
  name           = "${var.name}-${each.key}"
  log_group_name = each.value.group
  pattern        = local.alerts[each.value.alert].pattern
  metric_transformation {
    name      = local.alerts[each.value.alert].metric
    namespace = "${var.name}/pipeline"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "alerts" {
  for_each            = local.alerts
  alarm_name          = "${var.name}-${each.key == "errors" ? "pipeline-errors" : "quarantine-alert"}"
  alarm_description   = each.value.text
  namespace           = "${var.name}/pipeline"
  metric_name         = each.value.metric
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_budgets_budget" "monthly" {
  name         = "${var.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

output "glue_log_group_prefix" { value = local.glue_log_prefix }
output "glue_output_log_group" { value = aws_cloudwatch_log_group.glue_output.name }
output "glue_error_log_group" { value = aws_cloudwatch_log_group.glue_error.name }
output "glue_log_group_arns" { value = [aws_cloudwatch_log_group.glue_output.arn, aws_cloudwatch_log_group.glue_error.arn] }
output "app_log_group" { value = aws_cloudwatch_log_group.app.name }
output "app_log_group_arn" { value = aws_cloudwatch_log_group.app.arn }
output "alerts_topic_arn" { value = aws_sns_topic.alerts.arn }
