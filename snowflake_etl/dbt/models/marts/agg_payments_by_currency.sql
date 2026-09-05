-- Grain: one row per (month, currency, country_code).
-- The finance/BI deliverable, deliberately a DIFFERENT shape from the streaming lakehouse's
-- hourly operational gold: MONTHLY grain, normalized to USD. usd_volume is the headline
-- (cross-currency revenue you can actually sum); avg_rate_to_usd exposes the FX drift that
-- motivated the normalization.
--
-- usd_volume is ATTEMPTED volume: every payment_status is included, matching gross_volume in
-- the streaming gold table. authorized_usd_volume is the authorized subset. Neither is net
-- settlement volume -- refunds have no medallion yet, so nothing here subtracts them.
-- Both describe current state: a payment now at `refunded` still contributes to usd_volume.
SELECT
    DATE_TRUNC('month', created_at)::DATE AS month,
    currency,
    country_code,
    COUNT(*)            AS payment_count,
    SUM(amount)         AS native_volume,
    SUM(usd_amount)     AS usd_volume,
    SUM(CASE WHEN payment_status = 'authorized' THEN usd_amount ELSE 0 END)
                        AS authorized_usd_volume,
    AVG(rate_to_usd)    AS avg_rate_to_usd
FROM {{ ref('fct_payments_usd') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
