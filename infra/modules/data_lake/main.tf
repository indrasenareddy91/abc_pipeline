# Optional "data lake" view: Glue Data Catalog tables over the Parquet layers s3://<bucket>/silver/<table>/ and
# quarantine/<table>/, queried with Athena (pay per query, ~$5 per TB scanned; our files are KBs).
# Crawlers run on demand only. The warehouse (Redshift) stays the source for the dashboard and KPIs.

variable "name" { type = string }
variable "bucket_name" { type = string }
variable "bucket_arn" { type = string }

locals { db = replace(var.name, "-", "_") }

resource "aws_glue_catalog_database" "lake" {
  name = local.db
}

resource "aws_iam_role" "crawler" {
  name = "${var.name}-glue-crawler"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "crawler_s3" {
  name = "read-silver-and-quarantine"
  role = aws_iam_role.crawler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${var.bucket_arn}/silver/*", "${var.bucket_arn}/quarantine/*"] },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = [var.bucket_arn] },
    ]
  })
}

resource "aws_glue_crawler" "lake" {
  for_each      = { silver = "silver_", quarantine = "quarantine_" }
  name          = "${var.name}-${each.key}"
  role          = aws_iam_role.crawler.arn
  database_name = aws_glue_catalog_database.lake.name
  table_prefix  = each.value
  s3_target {
    path       = "s3://${var.bucket_name}/${each.key}/"
    exclusions = ["*.md"]
  }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
  configuration = jsonencode({ Version = 1.0, Grouping = { TableLevelConfiguration = 2 } })
}

resource "aws_athena_workgroup" "lake" {
  name          = var.name
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 1073741824 # 1 GB safety cap
    result_configuration {
      output_location = "s3://${var.bucket_name}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}

output "glue_database" { value = aws_glue_catalog_database.lake.name }
output "athena_workgroup" { value = aws_athena_workgroup.lake.name }
