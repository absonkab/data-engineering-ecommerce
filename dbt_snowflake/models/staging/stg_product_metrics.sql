-- Staging model for product-level e-commerce metrics.
-- Grain: one row per product and one-hour time window.

SELECT
    "product_id" AS product_id,
    "window_start" AS window_start,
    "window_end" AS window_end,
    "total_events" AS total_events,
    "views" AS views,
    "add_to_carts" AS add_to_carts,
    "purchases" AS purchases,
    "revenue" AS revenue,
    "unique_users" AS unique_users,
    "unique_viewers" AS unique_viewers,
    "unique_buyers" AS unique_buyers,
    "conversion_rate" AS conversion_rate

FROM {{ source('raw', 'RAW_PRODUCT_METRICS') }}