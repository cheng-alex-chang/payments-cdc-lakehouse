from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC     = "cdc.public.payments"
BRONZE_TABLE    = "iceberg.analytics.payments_bronze"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/bronze"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    LOGGER.info("Starting bronze Kafka ingestion from topic '%s'", KAFKA_TOPIC)
    spark = (
        SparkSession.builder
        .appName("bronze-from-kafka")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",          "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",     "hive")
        .config("spark.sql.catalog.iceberg.uri",      "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.iceberg.warehouse","hdfs://namenode:9000/warehouse")
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.analytics")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            kafka_key       STRING,
            kafka_value     STRING,
            kafka_topic     STRING,
            kafka_partition INT,
            kafka_offset    BIGINT,
            kafka_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(kafka_timestamp))
    """)

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("kafka_value"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
    )

    query = (
        stream.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .toTable(BRONZE_TABLE)
    )
    query.awaitTermination()
    LOGGER.info("Bronze streaming job completed")
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
