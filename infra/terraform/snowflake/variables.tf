variable "snowflake_admin_role" {
  description = "Snowflake role Terraform runs as. Needs CREATE INTEGRATION (ACCOUNTADMIN on a trial)."
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "aws_region" {
  description = "AWS region for the S3 lake bucket."
  type        = string
  default     = "us-east-1"
}

variable "database" {
  description = "Snowflake database holding the RAW + ANALYTICS schemas."
  type        = string
  default     = "PAYMENTS"
}

variable "warehouse" {
  description = "Virtual warehouse for the ELT. XS + auto-suspend to stay near $0 on a trial."
  type        = string
  default     = "PAYMENTS_WH"
}

variable "etl_role" {
  description = "Functional role granted on the database/warehouse for the pipeline."
  type        = string
  default     = "PAYMENTS_ETL_ROLE"
}

variable "s3_bucket" {
  description = <<-EOT
    Globally-unique S3 bucket name for the raw lake. REQUIRED -- no default on purpose.

    It used to default to "payments-lake-changeme". Unset, that is not a harmless
    placeholder: Terraform plans to repoint the storage integration and the external stage
    at a bucket that does not exist, so COPY INTO starts failing against a live warehouse.
    An unattended apply would do it silently. A missing value must stop the run instead.
  EOT
  type        = string
}

variable "stage_name" {
  description = "External stage name the loader/DAG COPY INTO from."
  type        = string
  default     = "PAYMENTS_LAKE_STAGE"
}

variable "iam_role_name" {
  description = "Name of the AWS IAM role Snowflake assumes to read the bucket."
  type        = string
  default     = "snowflake-payments-lake"
}

variable "etl_role_users" {
  description = <<-EOT
    Snowflake users to grant PAYMENTS_ETL_ROLE to. Without at least one, the role owns every
    privilege the pipeline needs and nobody can assume it -- connecting with
    SNOWFLAKE_ROLE=PAYMENTS_ETL_ROLE fails with "not granted to this user". Creating a
    functional role is only half the job; it has to reach a principal.
  EOT
  type        = list(string)
  # No default. An empty list is a valid *intent* ("grant to nobody") and therefore
  # indistinguishable from a forgotten variable -- and Terraform then plans to DESTROY the
  # grant the pipeline authenticates with. Pass [] explicitly if that is really what you mean.
}
