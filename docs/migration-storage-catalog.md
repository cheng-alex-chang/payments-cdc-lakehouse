# Migration — HDFS to object storage, Hive Metastore to an Iceberg REST catalog

The lakehouse was built on HDFS with a Hive Metastore because that is the stack Iceberg grew up in
and the one most tutorials assume. It worked, and running it taught more than skipping it would
have. It is also the wrong choice for anything that has to be operated.

This page records what changed, why, and what it measurably cost or saved. The "before" column was
captured on the last HDFS build before any of this began — those numbers cannot be reconstructed
after the fact, which is why they were taken first.

## Why move

**Hive Metastore is the legacy component, not HDFS.** HDFS is merely unnecessary here: the cluster
runs one node, Iceberg does not care what object store sits underneath it, and every cloud
deployment of this architecture would use S3 anyway. The metastore is the part that actively costs
you — Thrift RPC, a relational database of its own to run and back up, no fine-grained access
control, and no credential vending.

**An Iceberg REST catalog replaces it with an HTTP contract.** Any engine that speaks the REST spec
can attach without a Thrift client, and the catalog can hand out scoped, short-lived storage
credentials rather than every engine holding permanent keys.

**Iceberg makes the migration tractable.** The table format is unchanged. Spark and Trino get
reconfigured, not replaced, and because the pipeline is replay-safe the tables are rebuilt from
Kafka and Postgres rather than copied — no dual-write window, no partial state.

## Measured result

Memory figures are the sum of declared `requests` and `limits` across every Deployment and
StatefulSet in `k8s/base`.

| Measure | Before (HDFS + Hive Metastore) | After | Change |
|---|---:|---:|---|
| Workloads (Deployments + StatefulSets) | 16 | _tbd_ | |
| Memory requests | 6,304 Mi (6.16 GiB) | _tbd_ | |
| Memory limits | 12,096 Mi (11.81 GiB) | _tbd_ | |
| PersistentVolumeClaims | 6 | _tbd_ | |
| Pods running | 18 | _tbd_ | |
| Resident memory, kind node | 6.12 GiB / 11.67 GiB (52%) | _tbd_ | |
| Wall clock, `k8s_up.sh` to all-ready | 5m 14s | _tbd_ | |

Live figures come from a clean `k8s_up.sh` on the last HDFS build, measured with
`docker stats --no-stream` on the kind node once `k8s_verify.sh` reported all 16 workloads ready.

### The number that matters

Declared memory limits totalled **11.81 GiB against a 12 GB Docker allocation**. The platform sat
within 200 MiB of its ceiling, which is why bringing it up required raising Docker's memory in the
first place, and why a single mis-sized JVM heap took the whole cluster down rather than one pod.

The four workloads removed here account for **2,240 Mi of requests and 4,480 Mi of limits**:

| Workload | Kind | Requests | Limits |
|---|---|---:|---:|
| `namenode` | StatefulSet | 768 Mi | 1,536 Mi |
| `datanode` | StatefulSet | 768 Mi | 1,536 Mi |
| `hive-metastore` | Deployment | 512 Mi | 1,024 Mi |
| `metastore-db` | StatefulSet | 192 Mi | 384 Mi |

Three of the six PVCs go with them. What replaces all of it is MinIO plus a REST catalog — two
workloads, one PVC.

## What was deleted

Beyond the four workloads, the migration removed a surprising amount of incidental weight:

- `config/hadoop/` — `core-site.xml` and `hdfs-site.xml`
- `k8s/base/overrides/hdfs-site.xml` — one of only three files that had to differ on Kubernetes,
  needed solely because the NameNode had to bind `0.0.0.0` and DataNode reverse-DNS checks had to be
  disabled for pod IPs
- `config/hive-metastore/` and its custom Docker image build
- The committed PostgreSQL JDBC driver, present only so the metastore could reach its database
- `config/trino/catalog/hive.properties`
- The `hdfs-init` Job and `scripts/init_hdfs.py`

The `overrides/` deletion is worth noting on its own: that directory exists to hold files that must
genuinely differ between Compose and Kubernetes, and HDFS accounted for a third of it.

## What broke on the way

**Phase 1 — the Spark image's user has no home directory.** `spark-submit --packages` resolves
through Ivy, which writes a cache under `$HOME`. The image sets `HOME=/nonexistent`, so the first
run died with `FileNotFoundException: /nonexistent/.ivy2/cache/...`. The repo had already solved
this for its own Spark Jobs — `spark.jars.ivy=/tmp/.ivy2` plus an explicit `HOME` in
`k8s/base/spark.yaml` — which is an argument for reading the existing manifests before writing a
new one.

**Phase 1 — `io-impl` does not cover the catalog.** Iceberg's `S3FileIO` handles data and metadata
*files*, but the `hadoop` catalog type resolves its own warehouse directory through the Hadoop
FileSystem API, which `S3FileIO` never sees. Configuring `io-impl` and an `s3://` warehouse
produced `UnsupportedFileSystemException: No FileSystem for scheme "s3"` — the catalog was asking
Hadoop for a filesystem that had not been registered.

The fix for the smoke test was `s3a://` with `hadoop-aws:3.3.4`, pinned to match the
`hadoop-client-api` version in `spark:3.5.8-python3` (a mismatch there produces much worse errors).
The interesting part is that this constraint disappears in Phase 2: a REST catalog does not live on
a filesystem, so nothing needs to resolve the warehouse path through Hadoop, and `S3FileIO` becomes
sufficient on its own. **The filesystem-catalog-on-object-storage combination that failed here is a
known anti-pattern anyway** — it cannot do atomic commits — so hitting the wall was a signal the
target architecture is the right one, not a detour around it.

**Phase 3 — the streaming checkpoint is the open problem.** Bronze migrated cleanly: 50,004 rows
written to Iceberg on MinIO through the REST catalog, readable from Trino. Silver does not, and the
reason is specific:

```
BadRequestException: Malformed request: Table does not exist or user does not have
permission to view it at location `s3://checkpoints/silver/sources/0/offsets/0`
in warehouse `03039098-...`
```

Silver reads bronze as an Iceberg stream. Iceberg's micro-batch source writes its offset log
through the **table's** FileIO, and under a REST catalog that FileIO is scoped to the warehouse —
so it asks the catalog to authorise a path that belongs to no table, and the catalog correctly
refuses. Disabling `s3.remote-signing-enabled` did not change it, which rules out request signing
as the mechanism.

This never arose on HDFS because the checkpoint filesystem and the table filesystem were the same
unscoped `hdfs://`. It is a real consequence of a catalog that governs storage access, not a
configuration slip. Candidate resolutions, none yet tried:

1. Move checkpoints inside the warehouse prefix so they fall under the catalog's scope.
2. Keep checkpoints on a PVC rather than object storage — small, node-local, and arguably where
   streaming offsets belong.
3. Give the streaming source a separate unscoped FileIO via `spark.sql.catalog` overrides.

Option 2 is probably right: checkpoints are engine state, not lakehouse data, and putting them in
the governed warehouse conflates the two.

## Verification

The migration is considered complete only when the same checks that passed on HDFS pass again:

- `bash scripts/k8s_verify.sh` — every workload ready, not merely created
- A full `payments_pipeline` DAG run, all tasks green
- `bronze = silver = gold = 50,004` reconciled through Trino across 8,764 hourly buckets
- The gold API answering with the same 50,004 and its snapshot-keyed cache registering hits
- `pytest` green with no cluster running

Reconciliation at the identical row count is the point. The data is rebuilt from source rather than
migrated, so matching the pre-migration number is what proves the rebuild was faithful.
