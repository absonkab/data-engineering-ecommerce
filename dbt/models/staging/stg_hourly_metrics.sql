-- Staging model for hourly ecommerce metrics.
--
-- This model provides a clean and stable SQL interface
-- on top of the hourly Gold table published by Spark.

SELECT
    window_start,
    window_end,
    total_events,
    views,
    add_to_carts,
    purchases,
    revenue,
    unique_users

FROM {{ source('serving', 'gold_hourly_metrics') }}