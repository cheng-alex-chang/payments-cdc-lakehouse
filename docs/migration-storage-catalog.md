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

Three workloads go, two arrive. Both halves belong in the arithmetic:

| | Workload | Requests | Limits |
|---|---|---:|---:|
| removed | `namenode` | 768 Mi | 1,536 Mi |
| removed | `datanode` | 768 Mi | 1,536 Mi |
| removed | `hive-metastore` | 512 Mi | 1,024 Mi |
| added | `minio` | 256 Mi | 512 Mi |
| added | `iceberg-rest` | 192 Mi | 512 Mi |
| | **net** | **−1,600 Mi** | **−3,072 Mi** |

That takes declared limits from 11.81 GiB to roughly **8.8 GiB** — off the ceiling, with headroom
for the first time.

**`metastore-db` is not removed.** It is reused as the catalog's database and renamed `catalog-db`;
the Postgres instance survives, only its tenant changes.

**PVC count does not improve: 6 before, 6 after.** Two go with HDFS, MinIO adds one, and Spark's
streaming checkpoints need one of their own once they can no longer live in a catalog-governed
bucket. Stating it plainly because the earlier draft of this page claimed a reduction that the
arithmetic does not support.

## What was deleted

Beyond the three workloads, the migration removed a surprising amount of incidental weight:

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

**Phase 3 — remote signing versus streaming checkpoints.** This cost four cluster runs and was
the only genuinely hard part of the migration. Worth recording in full, because the wrong diagnosis
survived three of those runs.

Silver reads bronze as an Iceberg stream, and Iceberg's micro-batch source writes its offset log
through the **table's** FileIO rather than through Spark's checkpoint manager. Under a REST catalog
that FileIO is configured by the catalog, per table. Lakekeeper was returning:

```
"s3.remote-signing-enabled": "true"
"s3.signer.endpoint": ".../tabular-id/019fb10d-.../v1/aws/s3/sign"
```

Every S3 request from that FileIO went to the catalog to be signed, at an endpoint bound to **one
table id** — so the checkpoint write was rejected, correctly:

```
Table does not exist or user does not have permission to view it
at location `s3://checkpoints/silver/sources/0/offsets/0`
```

Two attempted fixes failed, and both failures were informative:

* Setting `s3.remote-signing-enabled=false` **on the engine** did nothing. The Iceberg REST spec
  gives the server's config precedence; you cannot opt out of signing from the client side.
* Moving checkpoints to a PersistentVolume produced `Invalid S3 URI, cannot determine scheme:
  file:/checkpoints/silver/...`. `S3FileIO` only parses `s3` URIs, and the offset log goes through
  it wherever the checkpoint lives. **The location was never the problem.**

The actual fix is one field on the warehouse's storage profile:
`"remote-signing-enabled": false`. With it off, the catalog stops signing and returns no access
key, so engines authenticate to storage with the credentials in their own environment. Checkpoints
in a separate bucket then work, and `S3FileIO` is kept — no `HadoopFileIO`, no `hadoop-aws` in the
table read path.

**How the wrong turn happened, since it is the more useful lesson.** I concluded that field did not
exist, and switched to `HadoopFileIO` on that basis. It did exist — my `grep` for `signing` matched
`remote-signing-url-style`, and the output was truncated before reaching `remote-signing-enabled`
two lines later. A filtered view of a config object was mistaken for the config object.

**What this means architecturally.** A catalog that governs storage access has to decide what a
checkpoint *is*. Lakekeeper treats every S3 path as table data; Structured Streaming treats the
checkpoint as query state. Neither is wrong, and nothing negotiated between them. Turning off
remote signing resolves it by moving that decision back to the engine: the catalog owns metadata,
engines authenticate to storage themselves. On real AWS the equivalent choice is STS with vended
credentials, where the same question arises as an IAM policy scope.

## Verification

The migration is considered complete only when the same checks that passed on HDFS pass again:

- `bash scripts/k8s_verify.sh` — every workload ready, not merely created
- A full `payments_pipeline` DAG run, all tasks green
- `bronze = silver = gold = 50,004` reconciled through Trino across 8,764 hourly buckets
- The gold API answering with the same 50,004 and its snapshot-keyed cache registering hits
- `pytest` green with no cluster running

Reconciliation at the identical row count is the point. The data is rebuilt from source rather than
migrated, so matching the pre-migration number is what proves the rebuild was faithful.
