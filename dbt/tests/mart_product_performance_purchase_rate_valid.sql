-- Business test:
-- Purchase rate cannot be negative.

SELECT
    window_start,
    window_end,
    product_id,
    purchase_rate

FROM {{ ref('mart_product_performance') }}

WHERE purchase_rate < 0
--   OR purchase_rate > 1