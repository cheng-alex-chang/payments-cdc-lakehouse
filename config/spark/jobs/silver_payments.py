from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    count,
    col,
    current_timestamp,
    from_unixtime,
    get_json_object,
    lit,
    lower,
    regexp_replace,
    sum as spark_sum,
    trim,
    upper,
)


BRONZE_TABLE    = "iceberg.analytics.payments_bronze"
SILVER_TABLE    = "iceberg.analytics.payments_silver"
DLQ_TABLE       = "iceberg.analytics.payments_silver_dlq"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/silver"

ALLOWED_PAYMENT_METHODS = (
    "apple_pay",
    "bank_transfer",
    "card",
    "google_pay",
    "paypal",
)

ALLOWED_PAYMENT_STATUSES = (
    "authorized",
    "cancelled",
    "chargeback",
    "failed",
    "pending",
    "refunded",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def _write_to_dlq(records: DataFrame, batch_id: int, reason: str) -> None:
    (
        records
        .select(
            col("kafka_value"),
            lit(batch_id).cast("long").alias("batch_id"),
            lit(reason).alias("error_reason"),
            current_timestamp().alias("ingested_at"),
        )
        .writeTo(DLQ_TABLE)
        .append()
    )
    LOGGER.warning("DLQ batch=%s reason=%s count=%s", batch_id, reason, records.count())


def _build_upserts(batch_df: DataFrame) -> DataFrame:
    return (
        batch_df
        .filter(get_json_object(col("kafka_value"), "$.op").isin("c", "u", "r"))
        .select(
            get_json_object(col("kafka_value"), "$.after.payment_id").cast("long").alias("payment_id"),
            get_json_object(col("kafka_value"), "$.after.merchant_id").cast("long").alias("merchant_id"),
            get_json_object(col("kafka_value"), "$.after.amount").cast("decimal(12,2)").alias("amount"),
            upper(trim(get_json_object(col("kafka_value"), "$.after.currency"))).alias("currency"),
            regexp_replace(lower(trim(get_json_object(col("kafka_value"), "$.after.payment_method"))), r"\s+", "_").alias("payment_method"),
            regexp_replace(lower(trim(get_json_object(col("kafka_value"), "$.after.payment_status"))), r"\s+", "_").alias("payment_status"),
            upper(trim(get_json_object(col("kafka_value"), "$.after.country_code"))).alias("country_code"),
            from_unixtime(get_json_object(col("kafka_value"), "$.after.created_at").cast("double") / 1_000_000).cast("timestamp").alias("created_at"),
            from_unixtime(get_json_object(col("kafka_value"), "$.after.updated_at").cast("double") / 1_000_000).cast("timestamp").alias("updated_at"),
            current_timestamp().alias("ingested_at"),
        )
    )


def _validate_upserts(upserts: DataFrame) -> None:
    quality_metrics = (
        upserts
        .select(
            spark_sum(col("payment_id").isNull().cast("int")).alias("null_payment_id"),
            spark_sum(col("merchant_id").isNull().cast("int")).alias("null_merchant_id"),
            spark_sum(col("amount").isNull().cast("int")).alias("null_amount"),
            spark_sum((col("amount") < 0).cast("int")).alias("negative_amount"),
            spark_sum(col("currency").isNull().cast("int")).alias("null_currency"),
            spark_sum((~col("currency").rlike("^[A-Z]{3}$")).cast("int")).alias("invalid_currency"),
            spark_sum(col("payment_method").isNull().cast("int")).alias("null_payment_method"),
            spark_sum((~col("payment_method").isin(*ALLOWED_PAYMENT_METHODS)).cast("int")).alias("invalid_payment_method"),
            spark_sum(col("payment_status").isNull().cast("int")).alias("null_payment_status"),
            spark_sum((~col("payment_status").isin(*ALLOWED_PAYMENT_STATUSES)).cast("int")).alias("invalid_payment_status"),
            spark_sum(col("country_code").isNull().cast("int")).alias("null_country_code"),
            spark_sum((~col("country_code").rlike("^[A-Z]{2}$")).cast("int")).alias("invalid_country_code"),
            spark_sum(col("created_at").isNull().cast("int")).alias("null_created_at"),
            spark_sum(col("updated_at").isNull().cast("int")).alias("null_updated_at"),
            spark_sum((col("updated_at") < col("created_at")).cast("int")).alias("updated_before_created"),
        )
        .collect()[0]
        .asDict()
    )

    duplicate_payment_ids = (
        upserts
        .groupBy("payment_id")
        .agg(count("*").alias("row_count"))
        .filter((col("payment_id").isNotNull()) & (col("row_count") > 1))
        .count()
    )

    failures = {name: value for name, value in quality_metrics.items() if value}
    if duplicate_payment_ids:
        failures["duplicate_payment_ids"] = duplicate_payment_ids

    if failures:
        raise ValueError(f"Silver data quality checks failed: {failures}")


def _upsert_to_silver(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession

    op_col = get_json_object(col("kafka_value"), "$.op")
    malformed = batch_df.filter(op_col.isNull())
    if not malformed.isEmpty():
        _write_to_dlq(malformed, batch_id, "null_op")

    known_ops = batch_df.filter(op_col.isNotNull())
    unexpected = known_ops.filter(~op_col.isin("c", "u", "r", "d"))
    if not unexpected.isEmpty():
        _write_to_dlq(unexpected, batch_id, "unexpected_op")

    processable = known_ops.filter(op_col.isin("c", "u", "r", "d"))

    upserts = _build_upserts(processable)
    if not upserts.isEmpty():
        _validate_upserts(upserts)
        upserts.createOrReplaceTempView("_silver_upserts")
        spark.sql(f"""
            MERGE INTO {SILVER_TABLE} t
            USING _silver_upserts s ON t.payment_id = s.payment_id
            WHEN MATCHED     THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    deletes = (
        processable
        .filter(op_col == "d")
        .select(get_json_object(col("kafka_value"), "$.before.payment_id").cast("long").alias("payment_id"))
        .filter(col("payment_id").isNotNull())
    )

    if not deletes.isEmpty():
        deletes.createOrReplaceTempView("_silver_deletes")
        spark.sql(f"""
            DELETE FROM {SILVER_TABLE}
            WHERE payment_id IN (SELECT payment_id FROM _silver_deletes)
        """)


def main() -> None:
    LOGGER.info("Starting silver transformation from %s", BRONZE_TABLE)
    spark = (
        SparkSession.builder
        .appName("silver-payments")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",          "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",     "hive")
        .config("spark.sql.catalog.iceberg.uri",      "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.iceberg.warehouse","hdfs://namenode:9000/warehouse")
        .getOrCreate()
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DLQ_TABLE} (
            kafka_value  STRING,
            batch_id     BIGINT,
            error_reason STRING,
            ingested_at  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingested_at))
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            payment_id     BIGINT,
            merchant_id    BIGINT,
            amount         DECIMAL(12,2),
            currency       STRING,
            payment_method STRING,
            payment_status STRING,
            country_code   STRING,
            created_at     TIMESTAMP,
            updated_at     TIMESTAMP,
            ingested_at    TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(created_at))
    """)

    query = (
        spark.readStream
        .format("iceberg")
        .load(BRONZE_TABLE)
        .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(_upsert_to_silver)
        .start()
    )
    query.awaitTermination()
    LOGGER.info("Silver streaming job completed")
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
