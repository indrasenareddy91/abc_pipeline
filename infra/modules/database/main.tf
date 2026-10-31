# MySQL on RDS: private (no public access), encrypted, TLS required.
# AWS generates the master password and keeps it in Secrets Manager (never in code or state).

variable "name" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "app_security_group_id" { type = string }
variable "engine_version" { type = string }
variable "instance_class" { type = string }
variable "storage_gb" { type = number }
variable "db_name" { type = string }
variable "backup_retention_days" { type = number }

resource "aws_security_group" "db" {
  name        = "${var.name}-db-sg"
  description = "MySQL: reachable only from the app server"
  vpc_id      = var.vpc_id
  tags        = { Name = "${var.name}-db-sg" }
}

resource "aws_vpc_security_group_ingress_rule" "mysql_from_app" {
  security_group_id            = aws_security_group.db.id
  referenced_security_group_id = var.app_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 3306
  to_port                      = 3306
  description                  = "MySQL from app server only"
}

resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-db-subnets"
  subnet_ids = var.private_subnet_ids
}

resource "aws_db_parameter_group" "this" {
  name   = "${var.name}-mysql"
  family = "mysql${var.engine_version}"
  parameter {
    name  = "require_secure_transport"
    value = "1"
  }
}

# Created by us (with retention) so RDS doesn't make a never-expiring one that survives destroy.
resource "aws_cloudwatch_log_group" "error" {
  name              = "/aws/rds/instance/${var.name}-mysql/error"
  retention_in_days = 14
}

resource "aws_db_instance" "this" {
  identifier                  = "${var.name}-mysql"
  engine                      = "mysql"
  engine_version              = var.engine_version
  instance_class              = var.instance_class
  allocated_storage           = var.storage_gb
  storage_type                = "gp3"
  storage_encrypted           = true
  db_name                     = var.db_name
  username                    = "admin_user"
  manage_master_user_password = true
  db_subnet_group_name        = aws_db_subnet_group.this.name
  vpc_security_group_ids      = [aws_security_group.db.id]
  parameter_group_name        = aws_db_parameter_group.this.name
  publicly_accessible         = false
  multi_az                    = false
  backup_retention_period     = var.backup_retention_days
  skip_final_snapshot         = true
  deletion_protection         = false
  apply_immediately           = true
  auto_minor_version_upgrade  = true

  iam_database_authentication_enabled = true
  copy_tags_to_snapshot               = true
  enabled_cloudwatch_logs_exports     = ["error"]

  depends_on = [aws_cloudwatch_log_group.error]
}

output "endpoint" { value = aws_db_instance.this.address }
output "port" { value = aws_db_instance.this.port }
output "db_name" { value = var.db_name }
output "secret_arn" { value = aws_db_instance.this.master_user_secret[0].secret_arn }
output "instance_id" { value = aws_db_instance.this.identifier }
