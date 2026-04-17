INSERT INTO merchants (merchant_id, merchant_name, country_code, category, created_at)
VALUES
    (1, 'Northwind Fashion', 'NL', 'fashion', NOW()),
    (2, 'Blue Harbor Travel', 'US', 'travel', NOW()),
    (3, 'Green Basket Foods', 'DE', 'grocery', NOW())
ON CONFLICT (merchant_id) DO NOTHING;

INSERT INTO payments (payment_id, merchant_id, shopper_id, amount, currency, payment_method, payment_status, country_code, created_at, updated_at)
VALUES
    (1001, 1, 501, 149.99, 'EUR', 'card', 'authorized', 'NL', NOW() - INTERVAL '2 day', NOW() - INTERVAL '2 day'),
    (1002, 2, 502, 499.50, 'USD', 'paypal', 'failed', 'US', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
    (1003, 1, 503, 89.00, 'EUR', 'card', 'authorized', 'BE', NOW() - INTERVAL '12 hour', NOW() - INTERVAL '12 hour'),
    (1004, 3, 504, 44.25, 'EUR', 'card', 'refunded', 'DE', NOW() - INTERVAL '6 hour', NOW() - INTERVAL '2 hour')
ON CONFLICT (payment_id) DO NOTHING;

INSERT INTO refunds (refund_id, payment_id, refund_amount, refund_reason, created_at)
VALUES
    (9001, 1004, 44.25, 'duplicate', NOW() - INTERVAL '1 hour')
ON CONFLICT (refund_id) DO NOTHING;
