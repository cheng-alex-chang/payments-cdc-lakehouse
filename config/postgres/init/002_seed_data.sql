INSERT INTO merchants (merchant_id, merchant_name, country_code, category, created_at)
VALUES
    (1, 'Northwind Fashion', 'NL', 'fashion', NOW()),
    (2, 'Blue Harbor Travel', 'US', 'travel', NOW()),
    (3, 'Green Basket Foods', 'DE', 'grocery', NOW()),
    (4, 'Cedar Health Market', 'CA', 'health', NOW()),
    (5, 'Metro Home Studio', 'GB', 'home', NOW()),
    (6, 'Sierra Outdoor Co', 'US', 'outdoors', NOW()),
    (7, 'Luna Beauty Lab', 'FR', 'beauty', NOW()),
    (8, 'Iberia Tech Depot', 'ES', 'electronics', NOW()),
    (9, 'Atlas Books', 'BE', 'books', NOW()),
    (10, 'Nordic Fitness Works', 'NL', 'fitness', NOW())
ON CONFLICT (merchant_id) DO NOTHING;

INSERT INTO payments (payment_id, merchant_id, shopper_id, amount, currency, payment_method, payment_status, country_code, created_at, updated_at)
VALUES
    (1001, 1, 501, 149.99, 'EUR', 'card', 'authorized', 'NL', NOW() - INTERVAL '2 day', NOW() - INTERVAL '2 day'),
    (1002, 2, 502, 499.50, 'USD', 'paypal', 'failed', 'US', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
    (1003, 1, 503, 89.00, 'EUR', 'card', 'authorized', 'BE', NOW() - INTERVAL '12 hour', NOW() - INTERVAL '12 hour'),
    (1004, 3, 504, 44.25, 'EUR', 'card', 'refunded', 'DE', NOW() - INTERVAL '6 hour', NOW() - INTERVAL '2 hour')
ON CONFLICT (payment_id) DO NOTHING;

WITH generated_payments AS (
    SELECT
        2000 + gs AS payment_id,
        ((gs - 1) % 10) + 1 AS merchant_id,
        700 + gs AS shopper_id,
        ROUND((25 + ((gs * 17) % 475) + (((gs * 13) % 100)::NUMERIC / 100)), 2)::NUMERIC(12, 2) AS amount,
        (ARRAY['EUR', 'USD', 'GBP', 'CAD'])[((gs - 1) % 4) + 1] AS currency,
        (ARRAY['card', 'paypal', 'apple_pay', 'bank_transfer', 'google_pay'])[((gs - 1) % 5) + 1] AS payment_method,
        (ARRAY['authorized', 'failed', 'authorized', 'pending', 'refunded', 'authorized', 'chargeback', 'cancelled'])[((gs - 1) % 8) + 1] AS payment_status,
        (ARRAY['NL', 'US', 'DE', 'BE', 'FR', 'GB', 'CA', 'ES'])[((gs - 1) % 8) + 1] AS country_code,
        (
            date_trunc('hour', NOW() - (((gs - 1) % 168) || ' hours')::INTERVAL)
            - ((((gs - 1) % 4) * 15) || ' minutes')::INTERVAL
        ) AS created_at
    FROM generate_series(1, 120) AS gs
)
INSERT INTO payments (
    payment_id,
    merchant_id,
    shopper_id,
    amount,
    currency,
    payment_method,
    payment_status,
    country_code,
    created_at,
    updated_at
)
SELECT
    payment_id,
    merchant_id,
    shopper_id,
    amount,
    currency,
    payment_method,
    payment_status,
    country_code,
    created_at,
    created_at + (((payment_id % 6) + 1) || ' hours')::INTERVAL AS updated_at
FROM generated_payments
ON CONFLICT (payment_id) DO NOTHING;

INSERT INTO refunds (refund_id, payment_id, refund_amount, refund_reason, created_at)
VALUES
    (9001, 1004, 44.25, 'duplicate', NOW() - INTERVAL '1 hour')
ON CONFLICT (refund_id) DO NOTHING;

INSERT INTO refunds (refund_id, payment_id, refund_amount, refund_reason, created_at)
SELECT
    9100 + payment_id,
    payment_id,
    amount,
    CASE payment_id % 4
        WHEN 0 THEN 'requested_by_customer'
        WHEN 1 THEN 'duplicate'
        WHEN 2 THEN 'suspected_fraud'
        ELSE 'inventory_shortfall'
    END,
    updated_at + INTERVAL '30 minutes'
FROM payments
WHERE payment_id BETWEEN 2001 AND 2120
  AND payment_status = 'refunded'
ON CONFLICT (refund_id) DO NOTHING;
