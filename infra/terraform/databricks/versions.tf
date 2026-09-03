terraform {
  # >= 1.10 for use_lockfile, which supersedes the DynamoDB lock table. Matches
  # infra/terraform/snowflake so the two states are managed the same way.
  required_version = ">= 1.10"

  # Remote state, added so CI can run plan/apply at all: local state cannot be shared
  # between a laptop and a runner, and a `terraform apply` against absent state would
  # try to recreate governance objects that already exist. Same bucket as the Snowflake
  # state under a different key. The bucket is bootstrapped outside this state
  # (chicken-and-egg): versioned and public-access-blocked, created once via the AWS API.
  # CI's validate job is unaffected -- `init -backend=false` skips backend init entirely.
  backend "s3" {
    bucket       = "payments-tfstate-alexchang-7f3k2"
    key          = "databricks/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
  }

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}
