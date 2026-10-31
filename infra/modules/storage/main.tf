# One private S3 bucket for all project data (the pipeline's DATA_ROOT):
#   raw/            landing zone, files exactly as given (never modified)  -> S3 event starts the pipeline
#   bronze/         every column as text + load metadata (Parquet)
#   silver/         current clean snapshot per table;  silver_changes/  rows each batch inserted/changed
#   quarantine/     rejected rows + plain-English reason
#   ops/            ingestion_log, profile, rules_log, row_counts (+ ops/server/ deploy scripts)
#   glue/           job scripts + engine.zip + project.zip (uploaded by scripts/deploy.sh)
#   athena-results/ only used by the optional data_lake module
# Private, encrypted, versioned (bad writes can be undone), HTTPS-only.

variable "name" { type = string }
variable "folders" { type = list(string) }
variable "force_destroy" { type = bool }

data "aws_caller_identity" "me" {}

resource "aws_s3_bucket" "data" {
  bucket        = "${var.name}-data-${data.aws_caller_identity.me.account_id}"
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_policy" "https_only" {
  bucket     = aws_s3_bucket.data.id
  depends_on = [aws_s3_bucket_public_access_block.data]
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyNonHttps"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# Old file versions are deleted after 30 days and half-finished uploads after 7,
# so versioning can't silently grow the bill.
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket     = aws_s3_bucket.data.id
  depends_on = [aws_s3_bucket_versioning.data]
  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = 30 }
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_s3_object" "folder" {
  for_each = toset(var.folders)
  bucket   = aws_s3_bucket.data.id
  key      = "${each.value}/"
  content  = ""
}

output "bucket_name" { value = aws_s3_bucket.data.bucket }
output "bucket_arn" { value = aws_s3_bucket.data.arn }
