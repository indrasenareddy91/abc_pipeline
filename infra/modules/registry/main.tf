# ECR: stores the app's Docker image (the same image runs locally and on the server).

variable "name" { type = string }
variable "force_delete" { type = bool }

resource "aws_ecr_repository" "app" {
  name                 = "${var.name}-app"
  image_tag_mutability = "MUTABLE"
  force_delete         = var.force_delete
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }
}

resource "aws_ecr_lifecycle_policy" "keep_recent" {
  repository = aws_ecr_repository.app.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the 10 most recent images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

output "repository_url" { value = aws_ecr_repository.app.repository_url }
output "repository_arn" { value = aws_ecr_repository.app.arn }
output "repository_name" { value = aws_ecr_repository.app.name }
