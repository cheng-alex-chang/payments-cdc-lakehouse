# Production-Readiness Notes

The pipelines are verified end-to-end (offline tests plus one live Snowflake/S3 run). Two gaps
remain between this and a real production deployment. Both are deliberate trade-offs for a
portfolio project on trial infrastructure, and both are named precisely enough to act on.

- **AWS credentials come from environment variables.** The S3 client reads `AWS_*`; production
  should use IAM roles — instance profile, IRSA, or `AssumeRole` — rather than long-lived access
  keys. The streaming half already does the equivalent right: its DAG authenticates through an
  Airflow Connection, and Kubernetes uses real `Secret` objects.

- **No data contract at the ingestion boundary.** The VARIANT load plus the staging cast can
  silently null-cast if the upstream schema drifts, and the Iceberg side has the matching gap —
  `CREATE TABLE IF NOT EXISTS` will not add a column Debezium starts emitting. Production would
  enforce an explicit contract where data enters, and fail loudly instead of writing nulls.

This list holds only what is *not* done. Work that has been completed is described where it lives —
key-pair auth in [snowflake_etl/README.md](../snowflake_etl/README.md), failure alerting and remote
Terraform state in the [README](../README.md) — so this page stays a short, true statement of what
is missing rather than a changelog.
