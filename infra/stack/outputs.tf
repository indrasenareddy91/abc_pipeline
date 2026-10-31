output "name" {
  description = "<project>-<env>: the argument for scripts/deploy.sh and scripts/e2e_aws.sh"
  value       = local.name
}

output "data_bucket" { value = module.storage.bucket_name }
output "data_root" { value = "s3://${module.storage.bucket_name}" }
output "image_repository" { value = module.registry.repository_url }

output "state_machine_arn" { value = module.trigger.state_machine_arn }
output "files_arrived_lambda" { value = module.trigger.lambda_name }
output "glue_jobs" { value = module.glue.job_names }
output "glue_role_arn" { value = module.glue.role_arn }
output "glue_log_groups" { value = [module.monitoring.glue_output_log_group, module.monitoring.glue_error_log_group] }
output "app_log_group" { value = module.monitoring.app_log_group }

output "redshift_workgroup" { value = module.redshift.workgroup_name }
output "redshift_namespace" { value = module.redshift.namespace_name }
output "redshift_database" { value = module.redshift.db_name }
output "redshift_secret_arn" { value = module.redshift.secret_arn }
output "redshift_endpoint" { value = module.redshift.endpoint }

output "server_instance_id" { value = var.enable_server ? module.compute[0].instance_id : null }
output "dashboard_url" {
  value = var.enable_server ? "http://${module.compute[0].public_ip}${var.dashboard_port == 80 ? "" : ":${var.dashboard_port}"}" : null
}
output "app_config_parameter" { value = var.enable_server ? module.compute[0].config_parameter : null }
output "dashboard_password" {
  value     = var.dashboard_password_enabled ? random_password.dashboard[0].result : null
  sensitive = true
}

output "db_endpoint" { value = var.enable_database ? module.database[0].endpoint : null }
output "db_secret_arn" { value = var.enable_database ? module.database[0].secret_arn : null }
output "github_deploy_role_arn" { value = length(module.github_cd) > 0 ? module.github_cd[0].role_arn : null }

locals {
  server_id = var.enable_server ? module.compute[0].instance_id : ""
  q         = "--workgroup-name ${module.redshift.workgroup_name} --database ${module.redshift.db_name} --secret-arn ${module.redshift.secret_arn}"
  cmds = [
    "Deploy code+image:  bash scripts/deploy.sh ${local.name}          (from the repo root)",
    "Upload a batch:     aws s3 cp project/sample_data/raw/day1/ s3://${module.storage.bucket_name}/raw/day1/ --recursive",
    "Run by hand:        aws stepfunctions start-execution --state-machine-arn ${module.trigger.state_machine_arn} --input '{\"batch\":\"day1\"}'",
    "Workflow runs:      aws stepfunctions list-executions --state-machine-arn ${module.trigger.state_machine_arn} --max-results 5",
    "Job logs:           aws logs tail ${module.monitoring.glue_output_log_group} --follow",
    "SQL (Data API):     aws redshift-data execute-statement ${local.q} --sql 'select count(*) from fact_transaction'",
    "End-to-end test:    bash scripts/e2e_aws.sh ${local.name} day1 day2",
    var.enable_server ? "Shell on server:    aws ssm start-session --target ${local.server_id}" : "",
    var.enable_server && var.dashboard_password_enabled ? "Dashboard password: terraform output -raw dashboard_password" : "",
    var.enable_data_lake ? "Data lake:          aws glue start-crawler --name ${local.name}-silver  -> Athena workgroup ${local.name}" : "",
  ]
}

output "useful_commands" { value = join("\n", compact(local.cmds)) }
