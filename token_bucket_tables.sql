-- Tables
-- DROP TABLE IF EXISTS token_buckets;
CREATE TABLE token_buckets (
    key_id      varchar(100) PRIMARY KEY,
    tokens      int4,
    last_refill timestamptz
);
-- DROP TABLE IF EXISTS token_rates;
CREATE TABLE token_rates (
    key_id         varchar(100) PRIMARY KEY,
    per_period     int4,
    period_seconds int4
);

