# Private network: 1 public subnet (dashboard server) + 3 private subnets in 3 AZs
# (Redshift Serverless needs subnets in 3 different AZs; the optional RDS database reuses them).
# No NAT gateway (costs ~$32/month): the private subnets have no internet route at all.
# A free S3 gateway endpoint lets the server reach S3 without leaving AWS's network.

variable "name" { type = string }
variable "cidr" { type = string }
variable "region" { type = string }
variable "dashboard_port" { type = number }
variable "dashboard_allowed_cidrs" { type = list(string) }
variable "enable_flow_logs" { type = bool }
variable "log_retention_days" { type = number }

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 3)
}

resource "aws_vpc" "this" {
  cidr_block           = var.cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.name}-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = cidrsubnet(var.cidr, 8, 1)
  availability_zone       = local.azs[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.name}-public" }
}

resource "aws_subnet" "private" {
  count             = 3
  vpc_id            = aws_vpc.this.id
  cidr_block        = cidrsubnet(var.cidr, 8, 10 + count.index)
  availability_zone = local.azs[count.index]
  tags              = { Name = "${var.name}-private-${count.index + 1}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
  tags = { Name = "${var.name}-public-rt" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-private-rt" }
}

resource "aws_route_table_association" "private" {
  count          = 3
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id, aws_route_table.private.id]
  tags              = { Name = "${var.name}-s3-endpoint" }
}

# App server firewall: only the dashboard port is open, and only to the allowed IPs.
# No SSH port: the server is managed through AWS SSM Session Manager.
resource "aws_security_group" "app" {
  name        = "${var.name}-app-sg"
  description = "Dashboard server: dashboard port only, no SSH"
  vpc_id      = aws_vpc.this.id
  tags        = { Name = "${var.name}-app-sg" }
}

resource "aws_vpc_security_group_ingress_rule" "dashboard" {
  for_each          = toset(var.dashboard_allowed_cidrs)
  security_group_id = aws_security_group.app.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = var.dashboard_port
  to_port           = var.dashboard_port
  description       = "Dashboard"
}

resource "aws_vpc_security_group_egress_rule" "all_out" {
  security_group_id = aws_security_group.app.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Outbound (package installs, AWS APIs)"
}

# The VPC's built-in default security group: remove all rules so nothing can use it by accident.
resource "aws_default_security_group" "locked" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-default-sg-locked" }
}

# VPC flow logs: record REJECTED traffic only (security audit trail, minimal cost).
resource "aws_cloudwatch_log_group" "flow" {
  count             = var.enable_flow_logs ? 1 : 0
  name              = "/${var.name}/vpc-flow"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "flow" {
  count = var.enable_flow_logs ? 1 : 0
  name  = "${var.name}-vpc-flow-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "vpc-flow-logs.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "flow" {
  count = var.enable_flow_logs ? 1 : 0
  name  = "${var.name}-vpc-flow"
  role  = aws_iam_role.flow[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"]
      Resource = ["${aws_cloudwatch_log_group.flow[0].arn}:*"]
    }]
  })
}

resource "aws_flow_log" "this" {
  count                = var.enable_flow_logs ? 1 : 0
  vpc_id               = aws_vpc.this.id
  traffic_type         = "REJECT"
  log_destination_type = "cloud-watch-logs"
  log_destination      = aws_cloudwatch_log_group.flow[0].arn
  iam_role_arn         = aws_iam_role.flow[0].arn
}

output "vpc_id" { value = aws_vpc.this.id }
output "app_security_group_id" { value = aws_security_group.app.id }
output "public_subnet_id" { value = aws_subnet.public.id }
output "private_subnet_ids" { value = aws_subnet.private[*].id }
