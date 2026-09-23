-- ============================================================
-- Serving Layer - Gold tables
-- ============================================================
--
-- PostgreSQL stores the business-ready Gold datasets so that
-- downstream consumers such as dashboards or APIs can access
-- the data without reading Parquet files directly.
-- ============================================================


-- ============================================================
-- Hourly metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_hourly_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,

    total_events BIGINT NOT NULL,
    views BIGINT NOT NULL,
    add_to_carts BIGINT NOT NULL,
    purchases BIGINT NOT NULL,

    revenue NUMERIC(18, 2) NOT NULL,
    unique_users BIGINT NOT NULL,

    PRIMARY KEY (window_start, window_end)
);

-- ============================================================
-- Product metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_product_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,

    product_id BIGINT NOT NULL,

    total_events BIGINT NOT NULL,
    views BIGINT NOT NULL,
    add_to_carts BIGINT NOT NULL,
    purchases BIGINT NOT NULL,

    revenue NUMERIC(18, 2) NOT NULL,

    unique_users BIGINT NOT NULL,
    unique_viewers BIGINT NOT NULL,
    unique_buyers BIGINT NOT NULL,

    conversion_rate NUMERIC(10, 4) NOT NULL,

    PRIMARY KEY (window_start, window_end, product_id)
);

-- ============================================================
-- User metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_user_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,

    user_id BIGINT NOT NULL,

    total_events BIGINT NOT NULL,
    views BIGINT NOT NULL,
    add_to_carts BIGINT NOT NULL,
    purchases BIGINT NOT NULL,

    revenue NUMERIC(18, 2) NOT NULL,

    unique_products BIGINT NOT NULL,

    purchase_rate NUMERIC(10, 4) NOT NULL,

    PRIMARY KEY (window_start, window_end, user_id)
);