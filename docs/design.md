# Design

## Overview

A local CDC data platform that ingests payment events from Postgres into a lakehouse using the Bronze / Silver / Gold medallion architecture. Changes are captured in real time via Debezium and processed incrementally with Apache Spark into Apache Iceberg tables on S3-compatible object storage, catalogued by an Iceberg REST catalog.

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
- **Pattern:** Structured Streaming with `trigger(availableNow=True)` and a checkpoint in the `s3://checkpoints` bucket
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
- **Source:** Silver Iceberg table only — gold reads nothing from bronze (strictly linear `bronze → silver → gold` lineage; the raw Debezium envelope is fully encapsulated by silver)
- **Pattern:** Batch `INSERT OVERWRITE` — a single `GROUP BY` aggregation over the current silver table that atomically replaces every gold row
- **Schema:** Hourly aggregates per `country_code` and `payment_method` — `payment_count`, exact-precision `gross_volume`, `auth_rate`
- **Partitioned by:** `days(payment_hour)`
- **Guarantee:** Full idempotent recompute from silver. Because it is a full atomic replace, hours whose payments were all deleted from silver are dropped from gold. Correct after inserts, updates, and deletes.

## Why Iceberg

Plain Parquet with `mode("overwrite")` rewrites the entire dataset on every run and cannot express row-level deletes from CDC. Iceberg adds:

- **MERGE INTO** — row-level upserts and deletes without full rewrites
- **Checkpointed streaming** — bronze and silver process only new data since the last run, removing the dependency on Kafka retaining full history
- **Partition evolution** — partition strategy can change without rewriting historical data
- **Time travel** — any snapshot is queryable; makes debugging data quality issues straightforward
- **ACID** — concurrent readers always see a consistent snapshot

## Incremental Processing

Bronze and silver use `trigger(availableNow=True)`. This is the "incremental batch" pattern: Spark reads all data accumulated since the last checkpoint, processes it, commits to Iceberg, and exits. The Airflow scheduler triggers each run on demand. No continuous streaming process is kept alive between runs.

Gold is a full idempotent recompute from silver: every run runs one `GROUP BY` aggregation over the current silver table and `INSERT OVERWRITE`s the whole gold table. Because it reads only silver and atomically replaces every row, it stays correct after silver updates and deletes (emptied hours simply disappear) without ever reaching back to bronze. This trades incrementality for clean linear lineage and self-healing determinism — the right default at this scale. When silver outgrows full rescans, the medallion-correct upgrade is a changelog read from silver (Iceberg `create_changelog_view`) that recomputes only changed hours, never a dependency back on bronze.

## CDC Delete Handling

Debezium sets `op=d` on delete events and populates `before` instead of `after`. The silver `foreachBatch` function splits each micro-batch into upserts (`op` in `c`, `u`, `r`) and deletes (`op=d`), issuing a `MERGE INTO` for the former and a `DELETE FROM` for the latter using `before.payment_id`. Records deleted in Postgres are removed from silver and recalculated out of gold on the next run.

## Known Limitations

- **Single Spark executor in both local runtimes.** Compose runs jobs on `local[*]` inside one Spark container. The Kubernetes overlay includes suspended Spark Job templates that also use `local[*]` inside the Spark image. A production-like Spark-on-Kubernetes driver/executor setup would require additional Spark submit configuration, image packaging, and executor service account tuning.
- **Single-node object storage.** MinIO runs one replica on one PVC, so a lost volume loses the warehouse. That is the same durability gap HDFS had at replication factor 1, and it is why this ran on HDFS first and then moved: the *fix* is now a configuration change rather than an architecture change, because Iceberg addresses S3 and the endpoint is a value. Pointing `S3_ENDPOINT` at managed S3, GCS, or ADLS makes block loss someone else's problem.
- **Kubernetes runs the pipeline end to end, including orchestration.** `scripts/k8s_up.sh` creates a local `kind` cluster and Kustomize renders the full platform shape: source Postgres, the catalog database, MinIO, the Iceberg REST catalog, Trino, Kafka (KRaft)/Kafka Connect, connector registration, Spark jobs, Airflow, the gold serving API, and observability. The data path is runtime-proven — bronze = silver = gold = 50,004 reconciled through Trino. Airflow drives it natively: the three Spark tasks become `KubernetesPodOperator` pods when `PIPELINE_RUNTIME=kubernetes`, and every other task talks HTTP to a service name that resolves in both runtimes, so no task shells into a Compose container. The remaining hardening is Spark itself: jobs run as a single `local[2]` driver rather than a production-like driver/executor split, which is a scale limitation rather than a runtime gap.
- **No schema evolution handling.** If the Postgres schema changes, Debezium will emit new fields but the silver `CREATE TABLE IF NOT EXISTS` will not add columns automatically. A schema migration step would be needed.
- **Delete-then-recreate within one batch.** If `op=d` and a later `op=c` for the same `payment_id` land in the same micro-batch, the upsert MERGE runs first and the DELETE then removes the recreated row. Rare in practice since Postgres usually reuses deterministic keys, but would need ordered per-key resolution to handle correctly.
- **Data quality scope.** Silver fails fast on invalid IDs, negative amounts, malformed country/currency codes, unsupported methods/statuses, and timestamps that move backward. A production platform would typically extend this with reconciliation against source totals and external alerting.
- **GDPR erasure.** Bronze is append-only. Right-to-be-forgotten would require an explicit erasure workflow that identifies affected Bronze offsets, deletes them, and expires old Iceberg snapshots.

## Deployment Runtimes

The primary runtime is Docker Compose because it brings up the full local data platform with one command and keeps local iteration fast.

An optional local Kubernetes path is available for infrastructure-managed deployments:

```
kind (scripts/k8s_up.sh) -> Kustomize overlay -> data-pipeline namespace
                                             -> shared ConfigMaps and Secrets
                                             -> stateful databases and object storage
                                             -> CDC, query, orchestration, and observability workloads
```

This path is intentionally no-cost and local-only. It shows how the platform begins to map from Compose services to Kubernetes primitives:

| Compose concept | Kubernetes concept |
|---|---|
| Docker network service names | Kubernetes Services |
| Docker volumes | PersistentVolumeClaims |
| bind-mounted config files | ConfigMaps |
| `.env` values | Secrets |
| long-running containers | Deployments or StatefulSets |
| one-off Spark commands | suspended Kubernetes Job templates or future Spark driver pods |

Kubernetes reached Compose parity and is now the primary path. The manifests cover the full platform shape, the data path is runtime-proven, and Airflow orchestrates it natively rather than through Compose-only container names. What remains is scale rather than parity: a production-like Spark driver/executor split, dependency-aware waits between stages, and service-specific inspection commands.

See [kubernetes.md](kubernetes.md) for commands, current verification steps, and the staged workload rollout plan.

## Orchestration

The Airflow DAG `payments_pipeline` runs the seven tasks in sequence:

```
init_object_store → init_catalog → validate_connector → bronze_load → silver_transform
  → gold_transform → publish_trino_tables → validate_trino
```

The DAG has no schedule (`schedule=None`) and is triggered manually or via the Airflow API. `max_active_runs=1` prevents concurrent runs from conflicting on the shared Iceberg tables.

## Snowflake FX ELT (batch + cloud warehouse)

A second pipeline runs the batch-into-a-warehouse paradigm over the same payments domain, adding a second source type (a pull-based REST API) and USD normalization. It is **ELT**, not ETL: load raw and untyped first, transform in the warehouse.

```
FX REST API (ECB/Frankfurter) ─┐
                               ├─ stage_to_s3 → S3 raw/<dataset>/dt=<date>/*.jsonl
Postgres payments ─────────────┘                    └─ COPY INTO → RAW.* (VARIANT)
                                                          └─ dbt ELT → ANALYTICS.*
```

### Layer contracts

- **RAW (`COPY INTO` VARIANT).** Each S3 JSON line lands verbatim in a single `VARIANT` column with `source_file` / `loaded_at` lineage. No reshaping at load time. Idempotent: `CREATE TABLE IF NOT EXISTS` plus COPY's load history (a file already loaded is skipped), so re-triggering the DAG never double-loads.
- **Staging (views).** Type the VARIANT into columns and `QUALIFY ROW_NUMBER()`-dedup to the latest version per key — replay-safe, the warehouse analogue of the silver dedup. `amount` is cast back from its exact string form to `NUMBER(12,2)` without passing through a float.
- **`dim_date` (table).** One row per calendar day spanning the payments + FX window — the conformed date dimension. Owns the calendar spine (Snowflake `GENERATOR`) plus reporting attributes (year/quarter/month/day-of-week/`is_weekend`).
- **`dim_fx_rates` (table).** One `rate_to_usd` per currency per **calendar** day. ECB publishes business days only, so `dim_date`'s spine × currencies is left-joined to actual rates and **forward-filled** (`LAST_VALUE ... IGNORE NULLS`, with a backward `FIRST_VALUE` for the leading edge); `is_filled` flags carried days. Guarantees every payment date resolves to a rate. Together with the fact this forms a Kimball-style star schema (see the ERD in `snowflake_etl/README.md`); grain is enforced by `unique`/`not_null` dbt tests.
- **`fct_payments_usd` (table).** `LEFT JOIN` payments to the dimension on `(currency, created_date)`; `usd_amount = ROUND(amount × rate_to_usd, 2)`. LEFT (not INNER) so an unmatched payment survives with a NULL `usd_amount` for validation to catch, rather than being silently dropped.
- **`agg_payments_by_currency` (table).** Monthly USD volume by currency/country — deliberately a different grain from the lakehouse's hourly operational gold. The two pipelines are two consumption tiers (operational vs. financial), not duplicates.
- **`validate` (gates).** Named PASS/FAIL checks the orchestrator asserts: fact reconciles to payments (N == N), no unmatched USD amount, no null/zero FX rate, USD identity (USD payments convert 1:1). The DAG's validate task raises on any FAIL.

### Orchestration & governance

The DAG `snowflake_fx_etl` stages the two sources to S3 in parallel (TaskFlow `@task`; payments extracted **incrementally** by `updated_at` over Airflow's data interval, full snapshot on manual triggers), fans into the RAW load (`SnowflakeOperator` over a managed `snowflake_default` connection), then `dbt run` builds the star schema (the fact is a dbt **incremental** model MERGEd by `payment_id` past a `MAX(updated_at)` watermark) and `dbt test` asserts the data-quality gates. Both DAGs alert on failure via a configurable webhook (`airflow/dags/alerts.py`). Snowflake auth is key-pair (`SNOWFLAKE_PRIVATE_KEY_PATH`; password fallback). Terraform (`infra/terraform/snowflake/`, remote S3 state with native lockfile) owns the database, schemas, warehouse, role/grants, and the AWS↔Snowflake storage integration + external stage — the same infra-vs-workload split as the Databricks port.

### Known limitations

Tracked in [production-readiness.md](production-readiness.md): env-based AWS creds (→ IAM roles) and data-contract enforcement at the ingestion boundary. (Key-pair auth, DAG alerting, and remote Terraform state are done.) The live load is run once against a Snowflake 30-day trial + S3 Free Tier for evidence, then torn down; the code persists and is otherwise verified offline (mocked S3, fake cursor, `terraform validate`).

## Monitoring

Prometheus scrapes three targets:

| Target | Metrics |
|---|---|
| `statsd-exporter:9102` | Airflow scheduler heartbeat, DAG run durations, task completions by state |
| `trino-exporter:8000` | Running / queued / finished / failed query counts, coordinator status |
| `prometheus:9090` | Prometheus self-metrics |

Grafana at `http://localhost:3001` reads from Prometheus and displays the Platform Overview dashboard.
