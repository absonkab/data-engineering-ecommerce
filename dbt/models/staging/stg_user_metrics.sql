-- Staging model for user-level ecommerce metrics.
--
-- This model provides a clean SQL interface
-- on top of the user Gold table published by Spark.

SELECT
    window_start,
    window_end,
    user_id,
    total_events,
    views,
    add_to_carts,
    purchases,
    revenue,
    unique_products,
    purchase_rate

FROM {{ source('serving', 'gold_user_metrics') }}