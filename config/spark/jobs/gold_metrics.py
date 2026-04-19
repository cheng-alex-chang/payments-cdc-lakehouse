from __future__ import annotations

import logging

from pyspark.sql import SparkSession


SILVER_TABLE = "iceberg.analytics.payments_silver"
GOLD_TABLE   = "iceberg.analytics.payment_metrics_gold"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    LOGGER.info("Starting gold aggregation from %s", SILVER_TABLE)
    spark = (
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

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            payment_hour   TIMESTAMP,
            country_code   STRING,
            payment_method STRING,
            payment_count  BIGINT,
            gross_volume   DOUBLE,
            auth_rate      DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(payment_hour))
    """)

    spark.sql(f"""
        MERGE INTO {GOLD_TABLE} t
        USING (
            SELECT
                date_trunc('hour', created_at)                                       AS payment_hour,
                country_code,
                payment_method,
                count(*)                                                              AS payment_count,
                sum(amount)                                                           AS gross_volume,
                avg(CASE WHEN payment_status = 'authorized' THEN 1.0 ELSE 0.0 END)   AS auth_rate
            FROM {SILVER_TABLE}
            GROUP BY 1, 2, 3
        ) s
        ON  t.payment_hour   = s.payment_hour
        AND t.country_code   = s.country_code
        AND t.payment_method = s.payment_method
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    LOGGER.info("Gold aggregation completed")
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
