variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "github_repository" {
  description = "owner/repo whose workflows may assume the role."
  type        = string
  default     = "cheng-alex-chang/payments-data-platform"
}

variable "main_branch" {
  description = "Branch the rehearse job runs on. That job has no GitHub environment, so its OIDC subject is the ref."
  type        = string
  default     = "main"
}

variable "prod_environment" {
  description = "GitHub Environment gating the release job; its OIDC subject is the environment, not the ref."
  type        = string
  default     = "prod"
}

variable "role_name" {
  type    = string
  default = "payments-ci-terraform"
}

variable "state_bucket" {
  description = "Remote Terraform state bucket, shared by the snowflake and databricks stacks."
  type        = string
  default     = "payments-tfstate-alexchang-7f3k2"
}

variable "lake_bucket" {
  description = "Raw lake bucket the Snowflake stack manages. Must match its s3_bucket variable."
  type        = string
  default     = "payments-lake-alexchang-2026"
}

variable "snowflake_iam_role_name" {
  description = "The one IAM role the Snowflake stack owns; must match its iam_role_name variable."
  type        = string
  default     = "snowflake-payments-lake"
}
