from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_unixtime,
    get_json_object,
    lower,
    regexp_replace,
    row_number,
    trim,
    upper,
)
from pyspark.sql.window import Window


BRONZE_PATH = "hdfs://namenode:9000/data/bronze/payments_cdc"
SILVER_PATH = "hdfs://namenode:9000/data/silver/payments"


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    LOGGER.info("Starting silver transformation from %s", BRONZE_PATH)
    spark = SparkSession.builder.appName("silver-payments").getOrCreate()

    bronze = spark.read.parquet(BRONZE_PATH)
    dedupe_window = Window.partitionBy("payment_id").orderBy(col("updated_at").desc())

    silver = (
        bronze.withColumn("payment_id", get_json_object(col("message_value"), "$.after.payment_id").cast("long"))
        .withColumn("merchant_id", get_json_object(col("message_value"), "$.after.merchant_id").cast("long"))
        .withColumn("shopper_id", get_json_object(col("message_value"), "$.after.shopper_id").cast("long"))
        .withColumn("amount", get_json_object(col("message_value"), "$.after.amount").cast("double"))
        .withColumn("currency", upper(trim(get_json_object(col("message_value"), "$.after.currency"))))
        .withColumn(
            "payment_method",
            regexp_replace(lower(trim(get_json_object(col("message_value"), "$.after.payment_method"))), r"\s+", "_"),
        )
        .withColumn(
            "payment_status",
            regexp_replace(lower(trim(get_json_object(col("message_value"), "$.after.payment_status"))), r"\s+", "_"),
        )
        .withColumn("country_code", upper(trim(get_json_object(col("message_value"), "$.after.country_code"))))
        .withColumn(
            "created_at",
            from_unixtime(
                get_json_object(col("message_value"), "$.after.created_at").cast("double") / 1000000
            ).cast("timestamp"),
        )
        .withColumn(
            "updated_at",
            from_unixtime(
                get_json_object(col("message_value"), "$.after.updated_at").cast("double") / 1000000
            ).cast("timestamp"),
        )
        .withColumn("row_number", row_number().over(dedupe_window))
        .filter(col("payment_id").isNotNull())
        .filter(col("row_number") == 1)
        .drop("row_number")
        .withColumn("ingested_at", current_timestamp())
    )

    LOGGER.info("Writing silver dataset to %s", SILVER_PATH)
    silver.write.mode("overwrite").parquet(SILVER_PATH)
    LOGGER.info("Silver dataset write completed")
    spark.stop()
    LOGGER.info("Spark session stopped for silver job")


if __name__ == "__main__":  # pragma: no cover
    main()
