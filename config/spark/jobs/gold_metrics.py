from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_trunc, from_unixtime, get_json_object, when


BRONZE_TABLE = "iceberg.analytics.payments_bronze"
SILVER_TABLE = "iceberg.analytics.payments_silver"
GOLD_TABLE   = "iceberg.analytics.payment_metrics_gold"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/gold"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def _build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold-metrics")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",          "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",     "hive")
        .config("spark.sql.catalog.iceberg.uri",      "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.iceberg.warehouse","hdfs://namenode:9000/warehouse")
        .getOrCreate()
    )


def _recompute_gold_partitions(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession

    affected_hours = (
        batch_df
        .select(
            get_json_object(col("kafka_value"), "$.op").alias("op"),
            get_json_object(col("kafka_value"), "$.after.created_at").cast("double").alias("after_created_at"),
            get_json_object(col("kafka_value"), "$.before.created_at").cast("double").alias("before_created_at"),
        )
        .select(
            date_trunc(
                "hour",
                from_unixtime(
                    when(col("op") == "d", col("before_created_at")).otherwise(col("after_created_at")) / 1_000_000
                ).cast("timestamp")
            ).alias("payment_hour")
        )
        .filter(col("payment_hour").isNotNull())
        .distinct()
    )

    if affected_hours.isEmpty():
        LOGGER.info("Gold batch %s contained no payment_hour changes", batch_id)
        return

    affected_hours.createOrReplaceTempView("_gold_affected_hours")
    spark.sql(f"""
        DELETE FROM {GOLD_TABLE}
        WHERE payment_hour IN (SELECT payment_hour FROM _gold_affected_hours)
    """)
    spark.sql(f"""
        INSERT INTO {GOLD_TABLE}
        SELECT
            date_trunc('hour', created_at)                                     AS payment_hour,
            country_code,
            payment_method,
            count(*)                                                            AS payment_count,
            CAST(sum(amount) AS DECIMAL(18,2))                                  AS gross_volume,
            avg(CASE WHEN payment_status = 'authorized' THEN 1.0 ELSE 0.0 END) AS auth_rate
        FROM {SILVER_TABLE}
        WHERE date_trunc('hour', created_at) IN (SELECT payment_hour FROM _gold_affected_hours)
        GROUP BY 1, 2, 3
    """)


def main() -> None:
    LOGGER.info("Starting gold aggregation from %s", SILVER_TABLE)
    spark = _build_spark_session()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            payment_hour   TIMESTAMP,
            country_code   STRING,
            payment_method STRING,
            payment_count  BIGINT,
            gross_volume   DECIMAL(18,2),
            auth_rate      DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(payment_hour))
    """)

    query = (
        spark.readStream
        .format("iceberg")
        .load(BRONZE_TABLE)
        .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(_recompute_gold_partitions)
        .start()
    )
    query.awaitTermination()

    LOGGER.info("Gold aggregation completed")
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
