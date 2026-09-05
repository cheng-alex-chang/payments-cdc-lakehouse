output "role_arn" {
  description = "Set this as the AWS_TERRAFORM_ROLE_ARN repository secret."
  value       = aws_iam_role.ci_terraform.arn
}

output "trusted_subjects" {
  description = "The only OIDC subjects that may assume the role."
  value = [
    "repo:${var.github_repository}:ref:refs/heads/${var.main_branch}",
    "repo:${var.github_repository}:environment:${var.prod_environment}",
  ]
}
