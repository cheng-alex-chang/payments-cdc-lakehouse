SELECT COUNT(*) AS silver_rows FROM iceberg.analytics.payments_silver;

SELECT COUNT(*) AS gold_rows FROM iceberg.analytics.payment_metrics_gold;

SELECT
    country_code,
    payment_method,
    SUM(payment_count) AS total_payments,
    SUM(gross_volume)  AS total_volume
FROM iceberg.analytics.payment_metrics_gold
GROUP BY 1, 2
ORDER BY total_volume DESC;
