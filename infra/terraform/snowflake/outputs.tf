output "s3_bucket" {
  description = "Raw lake bucket name (set as SNOWFLAKE_LAKE_BUCKET for the stager)."
  value       = aws_s3_bucket.lake.bucket
}

output "stage_name" {
  description = "Fully qualified external stage the loader COPY INTO from."
  value       = "${snowflake_database.payments.name}.${snowflake_schema.raw.name}.${snowflake_stage.lake.name}"
}

output "storage_integration" {
  description = "Storage integration wiring S3 to Snowflake."
  value       = snowflake_storage_integration.lake.name
}

output "snowflake_iam_user_arn" {
  description = "Snowflake-generated IAM user the AWS role trusts (useful for verifying the wiring)."
  value       = snowflake_storage_integration.lake.storage_aws_iam_user_arn
}
