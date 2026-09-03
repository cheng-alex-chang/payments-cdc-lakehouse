# Production-Readiness Notes

The pipelines are verified end-to-end (offline tests plus one live Snowflake/S3 run). What follows
is every gap between this repo and a real production deployment that I know about. They are
deliberate trade-offs for a portfolio project on a single-node local cluster, not oversights, and
each is stated precisely enough that you can check it against the code right now.

## Security

- **Containers run as root.** No manifest sets a `securityContext` at all — the one that did was
  `hdfs.yaml`, deleted in the storage migration; nothing declares
  `runAsNonRoot`, `readOnlyRootFilesystem`, or drops capabilities, so the namespace would fail the
  Pod Security Standards *restricted* profile. Fixing it properly means finding the right UID for
  each upstream image and testing every volume mount against it — worth doing on a cluster you can
  iterate on, not blind.

- **No NetworkPolicy.** Every pod in `data-pipeline` can reach every other, so a compromised
  exporter could talk straight to Postgres. Production would default-deny and allow only the edges
  the architecture actually uses.

- **The gold API is unauthenticated.** `api/` is read-only and its query surface is bounded
  (`MAX_PAGE_SIZE`, keyset cursors, no free-form SQL), but anything that can route to it can read
  every payment aggregate. Production would put it behind a gateway or require a token, and add
  per-caller rate limiting.

- **AWS credentials come from environment variables at runtime.** The S3 client reads `AWS_*`;
  production should use IAM roles — instance profile, IRSA, or `AssumeRole` — rather than
  long-lived access keys. The streaming half already does the equivalent right: its DAG
  authenticates through an Airflow Connection, and Kubernetes uses real `Secret` objects. Note the
  boundary: the *deployment* path is federated (`release-cloud.yml` assumes an IAM role through
  GitHub OIDC, so no AWS secret is stored), but that says nothing about how the running pipeline
  authenticates, which is what this bullet is about.

## Data

- **No data contract at the ingestion boundary.** The VARIANT load plus the staging cast can
  silently null-cast if the upstream schema drifts, and the Iceberg side has the matching gap —
  `CREATE TABLE IF NOT EXISTS` will not add a column Debezium starts emitting. Production would
  enforce an explicit contract where data enters, and fail loudly instead of writing nulls.

- **No backup or restore path.** Postgres, the catalog database, and Kafka sit on
  `volumeClaimTemplates`, and MinIO on a PVC, with no snapshot schedule and no tested restore. The
  catalog database is the one that stings: losing it loses every table's metadata pointer while the
  data files sit intact in object storage. For a local cluster the recovery procedure is
  `k8s_down.sh` and rebuild; that is not a recovery procedure.

## Not on this list

Single-replica everything, and Spark running as one `local[2]` driver rather than a driver/executor
split. Both are scale properties of a laptop-sized cluster rather than things the code gets wrong;
the Spark one is described in [design.md](design.md).

This page holds only what is *not* done — completed work is described where it lives, so this stays
a short true statement of what is missing rather than a changelog.
