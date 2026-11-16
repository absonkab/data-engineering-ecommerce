-- Staging model for user-level e-commerce metrics.
-- Grain: one row per user and one-hour time window.

SELECT
    "user_id" AS user_id,
    "window_start" AS window_start,
    "window_end" AS window_end,
    "total_events" AS total_events,
    "views" AS views,
    "add_to_carts" AS add_to_carts,
    "purchases" AS purchases,
    "revenue" AS revenue,
    "unique_products" AS unique_products,
    "purchase_rate" AS purchase_rate

FROM {{ source('raw', 'RAW_USER_METRICS') }}