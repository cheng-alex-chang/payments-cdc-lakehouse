# The trust anchor that lets release-cloud.yml run Terraform without a long-lived AWS key.
#
# Bootstrap stack, applied by hand: CI cannot create the credential CI needs.
#
# State lives in the same S3 bucket as the other stacks, under its own key. That is not
# circular -- the role this stack creates is for GitHub Actions, while a human applying it
# authenticates as themselves and already has direct bucket access. Keeping the state on one
# laptop instead would mean losing it makes Terraform forget the role and the OIDC provider
# entirely, leaving an `import` or a hand-rebuild as the only recovery.
#
# Scope is deliberately narrow. The role trusts two OIDC subjects and no others:
#   * ref:refs/heads/main  -- the `rehearse` job, which has no GitHub environment
#   * environment:prod     -- the `release` job, which does
# A pull request from a fork, a run on another branch, or another repository entirely all
# fail the trust policy rather than reaching AWS.

terraform {
  required_version = ">= 1.10"

  backend "s3" {
    bucket       = "payments-tfstate-alexchang-7f3k2"
    key          = "github-oidc/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.100"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

# GitHub's OIDC issuer. One per account; every repository's role trusts this same provider.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # AWS stopped verifying this thumbprint for GitHub's issuer in 2023, but the argument is
  # still required. This is GitHub's long-standing intermediate CA fingerprint.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_repository}:ref:refs/heads/${var.main_branch}",
        "repo:${var.github_repository}:environment:${var.prod_environment}",
      ]
    }
  }
}

resource "aws_iam_role" "ci_terraform" {
  name               = var.role_name
  description        = "GitHub Actions OIDC role for release-cloud.yml Terraform runs."
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

# What the workflow's Terraform runs actually touch: the remote state, the raw lake bucket,
# and the one IAM role the Snowflake stack manages for the storage integration.
data "aws_iam_policy_document" "ci_terraform" {
  # Remote state for both stacks (snowflake/ and databricks/ share the bucket).
  statement {
    sid       = "TerraformState"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["arn:aws:s3:::${var.state_bucket}/*"]
  }

  statement {
    sid       = "TerraformStateList"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketVersioning", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.state_bucket}"]
  }

  # The raw lake bucket the Snowflake stack creates and configures.
  statement {
    sid    = "LakeBucket"
    effect = "Allow"
    actions = [
      "s3:CreateBucket", "s3:DeleteBucket", "s3:ListBucket",
      "s3:GetBucket*", "s3:PutBucket*", "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
    ]
    resources = [
      "arn:aws:s3:::${var.lake_bucket}",
      "arn:aws:s3:::${var.lake_bucket}/*",
    ]
  }

  # Scoped to the single role the Snowflake stack owns -- not blanket IAM write. Terraform
  # reads it on every plan and rewrites its trust policy whenever the storage integration is
  # replaced, so read and write are both needed, on this role only.
  statement {
    sid    = "SnowflakeIntegrationRole"
    effect = "Allow"
    actions = [
      "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:TagRole", "iam:UntagRole",
      "iam:UpdateAssumeRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
      "iam:GetRolePolicy", "iam:PutRolePolicy", "iam:DeleteRolePolicy",
    ]
    resources = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.snowflake_iam_role_name}"]
  }
}

resource "aws_iam_role_policy" "ci_terraform" {
  name   = "ci-terraform"
  role   = aws_iam_role.ci_terraform.id
  policy = data.aws_iam_policy_document.ci_terraform.json
}
