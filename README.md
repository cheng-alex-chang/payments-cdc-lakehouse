# Local CDC Data Platform

A local data engineering project built around:

`Postgres -> Debezium/Kafka Connect -> Kafka -> PySpark -> Iceberg on HDFS -> Trino`

with:

- `Airflow` for orchestration
- `Hive Metastore` as the Iceberg catalog
- `Metabase` for ad hoc dashboards
- `Prometheus + Grafana` for platform monitoring and a seeded payments demo dashboard
- `pytest` for tests

See [docs/design.md](docs/design.md) for layer contracts, incremental processing, CDC delete handling, and known limitations.

## Architecture

```text
Postgres
  -> Debezium / Kafka Connect            CDC via logical replication
  -> Kafka topic: cdc.public.payments
  -> Spark bronze job                    Structured Streaming -> Iceberg append
  -> Spark silver job                    foreachBatch -> Iceberg MERGE / DELETE
  -> Spark gold job                      Batch SQL -> Iceberg MERGE
  -> Trino                               SQL over Iceberg via Hive Metastore
```

## Bronze, Silver, Gold

`Bronze`
- Raw Kafka envelope written to Iceberg, append-only
- Streaming with `trigger(availableNow=True)` and HDFS checkpoint
- PII fields (`shopper_id`) hashed with SHA-256 before the write so PII never lands in the lakehouse

`Silver`
- Canonical payment records, typed and normalised
- Dedup by `payment_id` (latest `updated_at`, tiebroken by `kafka_offset`) before MERGE so reruns and full Kafka replays converge to the same current state
- `foreachBatch` issues `MERGE INTO` for upserts (`op` in c, u, r) and `DELETE FROM` for Debezium deletes (`op=d`)

`Gold`
- Hourly aggregates per country and payment method (count, gross volume, auth rate)
- Idempotent full recalculation via `MERGE INTO` on every run

## Repo Layout

```text
airflow/dags/                  Airflow DAGs
config/airflow/                Airflow Docker image
config/connect/                Debezium connector config
config/grafana/                Grafana provisioning and dashboards
config/hadoop/                 Hadoop config
config/hive-metastore/         Hive metastore config
config/postgres/init/          Postgres schema and seed data
config/prometheus/             Prometheus config
config/spark/jobs/             Spark jobs
config/statsd/                 StatsD exporter mapping for Airflow metrics
config/trino/                  Trino config and Iceberg catalog
config/trino-exporter/         Custom Trino REST -> Prometheus exporter
docs/                          Design docs
scripts/                       Helper scripts
sql/trino/                     Trino validation SQL
tests/                         Unit tests
```

## Run Locally

### Prerequisites

- `Docker Desktop`
- `docker compose`
- `Python 3`

### Start the platform

```bash
docker compose up -d
```

All long-running services use `restart: unless-stopped` and expose healthchecks (Kafka, Zookeeper, NameNode, DataNode, Trino, Airflow, Postgres variants). Dependent services wait on `condition: service_healthy` before starting, so the stack self-recovers from individual container crashes without manual intervention.

### Register or refresh the Debezium connector

```bash
bash scripts/register_connector.sh
```

### Main URLs

- Airflow: `http://localhost:8088`
- Kafka Connect: `http://localhost:8083`
- Trino: `http://localhost:8080`
- HDFS NameNode UI: `http://localhost:9870`
- Metabase: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001`

### Load richer demo data

Fresh environments get the expanded demo seed automatically. If your containers and volumes already exist, reload the sample dataset into Postgres with:

```bash
python3 scripts/load_demo_data.py
```

Then trigger the Airflow DAG again so bronze, silver, and gold pick up the new CDC events.

### Run tests

```bash
source .venv/bin/activate
pytest --cov --cov-report=term-missing
```

## Airflow Pipeline

The main DAG is `airflow/dags/payments_pipeline.py`.

It runs:

1. `init_hdfs`
2. `validate_connector`
3. `bronze_load`
4. `silver_transform`
5. `gold_transform`
6. `publish_trino_tables`
7. `validate_trino`

Manual trigger:

```bash
docker exec dp-airflow-webserver airflow dags trigger payments_pipeline
```

## Demo Flow

### 1. Show the source data in Postgres

```bash
docker exec dp-postgres psql -U dataeng -d payments -c "SELECT payment_id, amount, payment_status, updated_at FROM payments ORDER BY payment_id;"
```

### 2. Change a source row

```bash
docker exec dp-postgres psql -U dataeng -d payments -c "UPDATE payments SET amount = 149.99, payment_status = 'authorized', updated_at = NOW() WHERE payment_id = 1001;"
```

### 3. Show the CDC event in Kafka

```bash
docker exec dp-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic cdc.public.payments \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000
```

### 4. Trigger the Airflow DAG

```bash
docker exec dp-airflow-webserver airflow dags trigger payments_pipeline
```

### 5. Confirm the DAG run

```bash
docker exec dp-airflow-webserver airflow dags list-runs -d payments_pipeline
```

### 6. Query the silver table in Trino

```bash
docker exec dp-trino trino --execute "SELECT payment_id, amount, payment_method, payment_status, created_at, updated_at FROM iceberg.analytics.payments_silver"
```

### 7. Query the gold table in Trino

```bash
docker exec dp-trino trino --execute "SELECT * FROM iceberg.analytics.payment_metrics_gold ORDER BY payment_hour, country_code, payment_method"
```

## Visualization

Grafana now provisions a `Payments Demo Overview` dashboard backed by the source Postgres database. Open `http://localhost:3001`, go to the `Data Platform` folder, and you should see charts for:

- total payments
- gross volume
- authorization rate
- refund events
- hourly volume trend
- payment method mix
- gross volume by country
- refunds over time

## Project Tour

This project is easiest to understand when viewed from three angles:

- orchestration in Airflow
- CDC and analytics results in Kafka and Trino
- business-facing metrics in Grafana

Airflow shows the pipeline running from source validation through bronze, silver, gold, and downstream validation.

![Airflow DAG showing the payments CDC pipeline run](docs/images/airflow-payments-pipeline.png)

Grafana shows the seeded demo data as business-facing metrics, including volume, authorization rate, refunds, and payment method mix.

![Grafana dashboard showing payment volume, authorization rate, refunds, and payment mix](docs/images/grafana-payments-demo-overview.png)

Trino shows the materialized gold layer directly, making it easy to inspect the hourly aggregates produced by the pipeline.

![Trino query results for the payment_metrics_gold Iceberg table](docs/images/trino-gold-metrics-query.png)
