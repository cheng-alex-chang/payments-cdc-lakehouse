CREATE SCHEMA IF NOT EXISTS hive.analytics
WITH (
    location = 'hdfs://namenode:9000/warehouse/analytics.db'
);

DROP TABLE IF EXISTS hive.analytics.payments_silver;
DROP TABLE IF EXISTS hive.analytics.payment_metrics_gold;

CREATE TABLE hive.analytics.payments_silver (
    payment_id BIGINT,
    merchant_id BIGINT,
    shopper_id BIGINT,
    amount DOUBLE,
    currency VARCHAR,
    payment_method VARCHAR,
    payment_status VARCHAR,
    country_code VARCHAR,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    ingested_at TIMESTAMP
)
WITH (
    external_location = 'hdfs://namenode:9000/data/silver/payments',
    format = 'PARQUET'
);

CREATE TABLE hive.analytics.payment_metrics_gold (
    payment_hour TIMESTAMP,
    country_code VARCHAR,
    payment_method VARCHAR,
    payment_count BIGINT,
    gross_volume DOUBLE,
    auth_rate DOUBLE
)
WITH (
    external_location = 'hdfs://namenode:9000/data/gold/payment_metrics',
    format = 'PARQUET'
);
