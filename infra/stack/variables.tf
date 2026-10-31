# ---- Change these per problem statement (in terraform.tfvars) ----
variable "project_name" {
  description = "Short lowercase name, prefixes every resource, e.g. retailbank, shopsphere"
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.project_name))
    error_message = "Use 3-21 chars: lowercase letters, digits, hyphens; start with a letter."
  }
}

variable "environment" {
  description = "dev / staging / prod (also part of the Terraform state key)"
  type        = string
  default     = "dev"
}

variable "alert_email" {
  description = "Gets budget alerts and pipeline-failure alerts"
  type        = string
}

variable "region" {
  type    = string
  default = "ap-south-1"
}

# ---- Pipeline trigger ----
variable "enable_file_trigger" {
  description = "Start the pipeline automatically when files land in s3://<bucket>/raw/<batch>/"
  type        = bool
  default     = true
}

variable "required_files" {
  description = "File name patterns that must ALL be in raw/<batch>/ before a run starts, e.g. [\"transactions*.csv\"]. Empty = run on every upload (one run per file!)."
  type        = list(string)
  default     = []
}

variable "schedule_expression" {
  description = "Also run on a schedule, e.g. \"cron(30 0 * * ? *)\" = 06:00 IST daily (cron is UTC). Empty = off."
  type        = string
  default     = ""
}

variable "schedule_batch" {
  description = "Batch folder the scheduled run processes (raw/<schedule_batch>/)"
  type        = string
  default     = "scheduled"
}

# ---- Glue (Spark jobs) ----
variable "glue_worker_type" {
  description = "G.1X = 4 vCPU / 16 GB per worker (1 DPU)"
  type        = string
  default     = "G.1X"
}

variable "glue_workers" {
  description = "2 is the minimum for G.1X (1 driver + 1 executor). ~$0.44 per DPU-hour, per second, 1-min minimum"
  type        = number
  default     = 2
}

variable "glue_timeout_minutes" {
  type    = number
  default = 30
}

# ---- Redshift Serverless (warehouse) ----
variable "redshift_db_name" {
  type    = string
  default = "warehouse"
}

variable "redshift_admin_username" {
  type    = string
  default = "admin_user"
}

variable "redshift_base_capacity" {
  description = "RPUs while running (8 = smallest)"
  type        = number
  default     = 8
}

variable "redshift_max_rpu_hours_per_day" {
  description = "Cost cap: the workgroup is DEACTIVATED for the rest of the day after this many RPU-hours (10 RPU-h at 8 RPU = 75 min of query time)"
  type        = number
  default     = 10
}

# ---- Dashboard server ----
variable "enable_server" {
  description = "EC2 dashboard server (Streamlit in Docker)"
  type        = bool
  default     = true
}

variable "dashboard_port" {
  type    = number
  default = 80
}

variable "dashboard_allowed_cidrs" {
  description = "Who can open the dashboard. Best: [\"<your-ip>/32\"]"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "dashboard_password_enabled" {
  description = "Protect the dashboard with a generated password (terraform output -raw dashboard_password)"
  type        = bool
  default     = true
}

variable "server_instance_type" {
  description = "t3.small = 2 vCPU, 2 GB RAM (~$0.022/hour). Enough for Streamlit + pandas"
  type        = string
  default     = "t3.small"
}

variable "server_disk_gb" {
  type    = number
  default = 20
}

# ---- Optional extras ----
variable "enable_database" {
  description = "Extra MySQL on RDS (not used by the pipeline: the warehouse is Redshift). Keep false."
  type        = bool
  default     = false
}

variable "db_engine_version" {
  description = "8.4 = current LTS. Do NOT use 8.0: past standard support, RDS bills Extended Support"
  type        = string
  default     = "8.4"
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "db_storage_gb" {
  type    = number
  default = 20
}

variable "db_name" {
  type    = string
  default = "appdb"
}

variable "db_backup_retention_days" {
  type    = number
  default = 1
}

variable "enable_data_lake" {
  description = "Glue Catalog crawlers + Athena over s3 silver/ and quarantine/ (query Parquet with SQL). Off by default."
  type        = bool
  default     = false
}

variable "github_repo" {
  description = "owner/repo for GitHub Actions deploys (OIDC). Empty = no CI/CD role"
  type        = string
  default     = ""
}

variable "create_github_oidc_provider" {
  description = "false if this AWS account already has the GitHub OIDC provider"
  type        = bool
  default     = true
}

# ---- Sizes / settings ----
variable "data_folders" {
  type    = list(string)
  default = ["raw", "bronze", "silver", "silver_changes", "quarantine", "ops", "glue", "athena-results"]
}

variable "vpc_cidr" {
  type    = string
  default = "10.30.0.0/16"
}

variable "budget_usd" {
  type    = number
  default = 20
}

variable "enable_vpc_flow_logs" {
  description = "Log rejected network traffic to CloudWatch (security audit). Costs cents at this scale"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "allow_destroy_with_data" {
  description = "true = terraform destroy also deletes bucket contents and images (fine for a hackathon)"
  type        = bool
  default     = true
}
