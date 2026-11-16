-- Staging model for hourly e-commerce metrics.
-- Grain: one row per one-hour time window.
-- This model provides a clean SQL interface over the RAW table.

SELECT
    "window_start" AS window_start,
    "window_end" AS window_end,
    "total_events" AS total_events,
    "views" AS views,
    "add_to_carts" AS add_to_carts,
    "purchases" AS purchases,
    "revenue" AS revenue,
    "unique_users" AS unique_users

FROM {{ source('raw', 'RAW_HOURLY_METRICS') }}