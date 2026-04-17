from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, date_trunc, sum, when


SILVER_PATH = "hdfs://namenode:9000/data/silver/payments"
GOLD_PATH = "hdfs://namenode:9000/data/gold/payment_metrics"


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    LOGGER.info("Starting gold aggregation from %s", SILVER_PATH)
    spark = SparkSession.builder.appName("gold-metrics").getOrCreate()

    payments = spark.read.parquet(SILVER_PATH)

    gold = (
        payments.withColumn("payment_hour", date_trunc("hour", col("created_at")))
        .groupBy("payment_hour", "country_code", "payment_method")
        .agg(
            count("*").alias("payment_count"),
            sum("amount").alias("gross_volume"),
            avg(when(payments.payment_status == "authorized", 1).otherwise(0)).alias("auth_rate"),
        )
    )

    LOGGER.info("Writing gold dataset to %s", GOLD_PATH)
    gold.write.mode("overwrite").parquet(GOLD_PATH)
    LOGGER.info("Gold dataset write completed")
    spark.stop()
    LOGGER.info("Spark session stopped for gold job")


if __name__ == "__main__":  # pragma: no cover
    main()
