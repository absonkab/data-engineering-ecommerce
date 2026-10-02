-- Business test:
-- Average revenue per purchase cannot be negative.

SELECT
    window_start,
    window_end,
    product_id,
    revenue_per_purchase

FROM {{ ref('mart_product_performance') }}

WHERE revenue_per_purchase < 0