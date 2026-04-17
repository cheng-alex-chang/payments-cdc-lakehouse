# Architecture Notes

## Core pipeline

`Postgres -> Debezium/Kafka Connect -> Kafka -> PySpark -> HDFS -> Hive -> Trino -> Metabase`

## Operational layer

- `Airflow` schedules and retries jobs
- `Grafana` visualizes health and freshness
- `Prometheus` stores metrics

## Bronze / Silver / Gold

- `bronze`: raw CDC events from Kafka
- `silver`: normalized payment-level records
- `gold`: dashboard-ready aggregates

## Suggested interview talking points

- source-of-truth OLTP data in Postgres
- CDC over polling for fresher ingestion
- separation of business dashboards and operational monitoring
- tests and validations built into orchestration
- Trino for fast analyst-friendly SQL over lake data
