from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BRONZE_PATH = "hdfs://namenode:9000/data/bronze/payments_cdc"
KAFKA_TOPIC = "cdc.public.payments"


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    LOGGER.info("Starting bronze Kafka ingestion from topic '%s'", KAFKA_TOPIC)
    spark = (
        SparkSession.builder.appName("bronze-from-kafka")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze = df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("message_value"),
    )

    LOGGER.info("Writing bronze dataset to %s", BRONZE_PATH)
    bronze.write.mode("overwrite").parquet(BRONZE_PATH)
    LOGGER.info("Bronze dataset write completed")
    spark.stop()
    LOGGER.info("Spark session stopped for bronze job")


if __name__ == "__main__":  # pragma: no cover
    main()
