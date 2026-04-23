# Design

## Overview

A local CDC data platform that ingests payment events from Postgres into a lakehouse using the Bronze / Silver / Gold medallion architecture. Changes are captured in real time via Debezium and processed incrementally with Apache Spark and Apache Iceberg.

## Data Flow

```
Postgres (OLTP)
  └─ Debezium / Kafka Connect        CDC via logical replication (pgoutput)
       └─ Kafka topic                cdc.public.payments
            └─ Bronze job            Structured Streaming → Iceberg append
                 └─ Silver job       Streaming foreachBatch → Iceberg MERGE INTO
                      └─ Gold job    Batch SQL → Iceberg MERGE INTO
                           └─ Trino  SQL query layer over Iceberg tables
```

## Layer Contracts

### Bronze
- **Source:** Kafka topic `cdc.public.payments`
- **Pattern:** Structured Streaming with `trigger(availableNow=True)` and HDFS checkpoint
- **Schema:** Raw Kafka envelope — `kafka_key`, `kafka_value` (Debezium JSON, PII hashed), `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`
- **Partitioned by:** `days(kafka_timestamp)`
- **PII:** `shopper_id` in both `before` and `after` sections is SHA-256 hashed before the Iceberg write so PII never lands in the lakehouse
- **Guarantee:** Append-only. Every CDC event is preserved exactly once. Checkpoint prevents re-processing on reruns.

### Silver
- **Source:** Bronze Iceberg table (Iceberg streaming source)
- **Pattern:** Streaming `foreachBatch` → dedup by `payment_id` (latest `updated_at`, tiebroken by `kafka_offset`) → `MERGE INTO` for inserts/updates, `DELETE FROM` for Debezium `op=d`
- **Schema:** Canonical payment record — typed, normalised text fields, exact-precision `DECIMAL(12,2)` amount, timestamps in microseconds converted to `TIMESTAMP`
- **Partitioned by:** `days(created_at)`
- **Replay safety:** Multiple CDC events per key in the same batch (replay from earliest offset, back-to-back updates) collapse to the latest version before MERGE, so reruns converge to the same current state as an incremental run
- **Guarantee:** Current state of each payment. Handles the full CDC contract: inserts, updates, and deletes.

### Gold
- **Source:** Bronze CDC events identify changed hours; silver provides the current-state rows for recomputation
- **Pattern:** Streaming `foreachBatch` on bronze → recompute only affected hourly partitions in gold
- **Schema:** Hourly aggregates per `country_code` and `payment_method` — `payment_count`, exact-precision `gross_volume`, `auth_rate`
- **Partitioned by:** `days(payment_hour)`
- **Guarantee:** Idempotent recomputation of only the hours touched by incoming CDC events. Correct after inserts, updates, and deletes.

## Why Iceberg

Plain Parquet with `mode("overwrite")` rewrites the entire dataset on every run and cannot express row-level deletes from CDC. Iceberg adds:

- **MERGE INTO** — row-level upserts and deletes without full rewrites
- **Checkpointed streaming** — bronze and silver process only new data since the last run, removing the dependency on Kafka retaining full history
- **Partition evolution** — partition strategy can change without rewriting historical data
- **Time travel** — any snapshot is queryable; makes debugging data quality issues straightforward
- **ACID** — concurrent readers always see a consistent snapshot

## Incremental Processing

Bronze and silver use `trigger(availableNow=True)`. This is the "incremental batch" pattern: Spark reads all data accumulated since the last checkpoint, processes it, commits to Iceberg, and exits. The Airflow scheduler triggers each run on demand. No continuous streaming process is kept alive between runs.

Gold uses the incoming CDC events to determine which `payment_hour` partitions changed. For each micro-batch it deletes the existing gold rows for those hours and recomputes them from the full current silver state. This avoids full-table rescans while staying correct after silver updates and deletes.

## CDC Delete Handling

Debezium sets `op=d` on delete events and populates `before` instead of `after`. The silver `foreachBatch` function splits each micro-batch into upserts (`op` in `c`, `u`, `r`) and deletes (`op=d`), issuing a `MERGE INTO` for the former and a `DELETE FROM` for the latter using `before.payment_id`. Records deleted in Postgres are removed from silver and recalculated out of gold on the next run.

## Known Limitations

- **Single Spark executor.** Jobs run on `local[*]` inside one container. Horizontal scaling would require a proper Spark cluster (YARN, Kubernetes) and external shuffle service.
- **Single-replica HDFS.** Replication factor 1 with one datanode. All long-running containers use `restart: unless-stopped` with healthchecks, so a crashed datanode comes back automatically and dependent services (Hive Metastore, Spark, Trino) wait on `condition: service_healthy` before talking to it — so transient container crashes self-recover. The residual gap is data durability: a lost datanode volume would still lose the blocks. Production would use replication factor 3+ across multiple datanodes, or managed object storage (S3, GCS, ADLS) where block loss is not possible.
- **No schema evolution handling.** If the Postgres schema changes, Debezium will emit new fields but the silver `CREATE TABLE IF NOT EXISTS` will not add columns automatically. A schema migration step would be needed.
- **Delete-then-recreate within one batch.** If `op=d` and a later `op=c` for the same `payment_id` land in the same micro-batch, the upsert MERGE runs first and the DELETE then removes the recreated row. Rare in practice since Postgres usually reuses deterministic keys, but would need ordered per-key resolution to handle correctly.
- **Data quality scope.** Silver fails fast on invalid IDs, negative amounts, malformed country/currency codes, unsupported methods/statuses, and timestamps that move backward. A production platform would typically extend this with reconciliation against source totals and external alerting.
- **GDPR erasure.** Bronze is append-only. Right-to-be-forgotten would require an explicit erasure workflow that identifies affected Bronze offsets, deletes them, and expires old Iceberg snapshots.

## Orchestration

The Airflow DAG `payments_pipeline` runs the seven tasks in sequence:

```
init_hdfs → validate_connector → bronze_load → silver_transform
  → gold_transform → publish_trino_tables → validate_trino
```

The DAG has no schedule (`schedule=None`) and is triggered manually or via the Airflow API. `max_active_runs=1` prevents concurrent runs from conflicting on the shared Iceberg tables.

## Monitoring

Prometheus scrapes three targets:

| Target | Metrics |
|---|---|
| `statsd-exporter:9102` | Airflow scheduler heartbeat, DAG run durations, task completions by state |
| `trino-exporter:8000` | Running / queued / finished / failed query counts, coordinator status |
| `prometheus:9090` | Prometheus self-metrics |

Grafana at `http://localhost:3001` reads from Prometheus and displays the Platform Overview dashboard.
