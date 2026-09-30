-- Staging model for product-level ecommerce metrics.
--
-- This model provides a clean SQL interface
-- on top of the product Gold table published by Spark.

SELECT
    window_start,
    window_end,
    product_id,
    total_events,
    views,
    add_to_carts,
    purchases,
    revenue,
    unique_users,
    unique_viewers,
    unique_buyers,
    conversion_rate

FROM {{ source('serving', 'gold_product_metrics') }}