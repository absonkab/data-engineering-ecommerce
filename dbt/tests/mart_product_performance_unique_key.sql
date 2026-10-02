-- Business test:
-- Each product must have exactly one row per time window.
--
-- The business grain of this mart is:
-- (window_start, window_end, product_id)

SELECT
    window_start,
    window_end,
    product_id,
    COUNT(*) AS row_count

FROM {{ ref('mart_product_performance') }}

GROUP BY
    window_start,
    window_end,
    product_id

HAVING COUNT(*) > 1