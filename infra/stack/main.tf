# RetailBank / any problem statement:
#   S3 raw/ -> Lambda -> Step Functions -> Glue bronze -> silver -> gold -> Redshift Serverless -> Streamlit (EC2)
locals {
  name      = "${var.project_name}-${var.environment}"
  glue_jobs = ["bronze", "silver", "gold"] # order matters: the workflow runs them in this order
}

module "network" {
  source                  = "../modules/network"
  name                    = local.name
  cidr                    = var.vpc_cidr
  region                  = var.region
  dashboard_port          = var.dashboard_port
  dashboard_allowed_cidrs = var.dashboard_allowed_cidrs
  enable_flow_logs        = var.enable_vpc_flow_logs
  log_retention_days      = var.log_retention_days
}

module "storage" {
  source        = "../modules/storage"
  name          = local.name
  folders       = var.data_folders
  force_destroy = var.allow_destroy_with_data
}

module "registry" {
  source       = "../modules/registry"
  name         = local.name
  force_delete = var.allow_destroy_with_data
}

module "monitoring" {
  source             = "../modules/monitoring"
  name               = local.name
  alert_email        = var.alert_email
  budget_usd         = var.budget_usd
  log_retention_days = var.log_retention_days
}

module "redshift" {
  source                = "../modules/redshift"
  name                  = local.name
  vpc_id                = module.network.vpc_id
  subnet_ids            = module.network.private_subnet_ids
  app_security_group_id = module.network.app_security_group_id
  db_name               = var.redshift_db_name
  admin_username        = var.redshift_admin_username
  base_capacity         = var.redshift_base_capacity
  max_rpu_hours_per_day = var.redshift_max_rpu_hours_per_day
}

module "glue" {
  source                 = "../modules/glue"
  name                   = local.name
  bucket_name            = module.storage.bucket_name
  bucket_arn             = module.storage.bucket_arn
  log_group_prefix       = module.monitoring.glue_log_group_prefix
  log_group_arns         = module.monitoring.glue_log_group_arns
  redshift_workgroup     = module.redshift.workgroup_name
  redshift_workgroup_arn = module.redshift.workgroup_arn
  redshift_db            = module.redshift.db_name
  redshift_secret_arn    = module.redshift.secret_arn
  worker_type            = var.glue_worker_type
  number_of_workers      = var.glue_workers
  timeout_minutes        = var.glue_timeout_minutes
  jobs                   = local.glue_jobs
}

module "trigger" {
  source              = "../modules/trigger"
  name                = local.name
  bucket_name         = module.storage.bucket_name
  bucket_arn          = module.storage.bucket_arn
  required_files      = var.required_files
  enable_file_trigger = var.enable_file_trigger
  glue_jobs           = local.glue_jobs
  glue_job_names      = module.glue.job_names
  glue_job_arns       = module.glue.job_arns
  alerts_topic_arn    = module.monitoring.alerts_topic_arn
  log_retention_days  = var.log_retention_days
}

module "schedule" {
  count             = var.schedule_expression != "" ? 1 : 0
  source            = "../modules/schedule"
  name              = local.name
  expression        = var.schedule_expression
  state_machine_arn = module.trigger.state_machine_arn
  batch             = var.schedule_batch
}

resource "random_password" "dashboard" {
  count   = var.dashboard_password_enabled ? 1 : 0
  length  = 20
  special = false
}

module "database" {
  count                 = var.enable_database ? 1 : 0
  source                = "../modules/database"
  name                  = local.name
  vpc_id                = module.network.vpc_id
  private_subnet_ids    = module.network.private_subnet_ids
  app_security_group_id = module.network.app_security_group_id
  engine_version        = var.db_engine_version
  instance_class        = var.db_instance_class
  storage_gb            = var.db_storage_gb
  db_name               = var.db_name
  backup_retention_days = var.db_backup_retention_days
}

module "compute" {
  count                  = var.enable_server ? 1 : 0
  source                 = "../modules/compute"
  name                   = local.name
  region                 = var.region
  instance_type          = var.server_instance_type
  root_volume_gb         = var.server_disk_gb
  subnet_id              = module.network.public_subnet_id
  security_group_id      = module.network.app_security_group_id
  bucket_name            = module.storage.bucket_name
  bucket_arn             = module.storage.bucket_arn
  ecr_repository_arn     = module.registry.repository_arn
  ecr_repository_url     = module.registry.repository_url
  app_log_group          = module.monitoring.app_log_group
  app_log_group_arn      = module.monitoring.app_log_group_arn
  pipeline_log_group     = module.monitoring.glue_output_log_group
  dashboard_port         = var.dashboard_port
  dashboard_password     = var.dashboard_password_enabled ? random_password.dashboard[0].result : ""
  state_machine_arn      = module.trigger.state_machine_arn
  redshift_workgroup     = module.redshift.workgroup_name
  redshift_workgroup_arn = module.redshift.workgroup_arn
  redshift_db            = module.redshift.db_name
  redshift_secret_arn    = module.redshift.secret_arn
  db_secret_arn          = var.enable_database ? module.database[0].secret_arn : ""
  db_host                = var.enable_database ? module.database[0].endpoint : ""
}

module "github_cd" {
  count                = var.github_repo != "" && var.enable_server ? 1 : 0
  source               = "../modules/github_cd"
  name                 = local.name
  github_repo          = var.github_repo
  create_oidc_provider = var.create_github_oidc_provider
  ecr_repository_arn   = module.registry.repository_arn
  instance_arn         = module.compute[0].instance_arn
  bucket_arn           = module.storage.bucket_arn
}

module "data_lake" {
  count       = var.enable_data_lake ? 1 : 0
  source      = "../modules/data_lake"
  name        = local.name
  bucket_name = module.storage.bucket_name
  bucket_arn  = module.storage.bucket_arn
}
