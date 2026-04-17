CREATE TABLE IF NOT EXISTS merchants (
    merchant_id BIGINT PRIMARY KEY,
    merchant_name TEXT NOT NULL,
    country_code TEXT NOT NULL,
    category TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id BIGINT PRIMARY KEY,
    merchant_id BIGINT NOT NULL REFERENCES merchants(merchant_id),
    shopper_id BIGINT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    payment_status TEXT NOT NULL,
    country_code TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS refunds (
    refund_id BIGINT PRIMARY KEY,
    payment_id BIGINT NOT NULL REFERENCES payments(payment_id),
    refund_amount NUMERIC(12, 2) NOT NULL,
    refund_reason TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

ALTER SYSTEM SET wal_level = logical;
