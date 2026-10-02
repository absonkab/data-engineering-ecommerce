-- Business test:
-- The number of purchases cannot be negative.

SELECT
    window_start,
    window_end,
    product_id,
    purchases

FROM {{ ref('mart_product_performance') }}

WHERE purchases < 0