-- Business test:
-- Revenue should never be negative.
--
-- A dbt singular test passes when this query returns zero rows.

SELECT
    window_start,
    window_end,
    product_id,
    revenue

FROM {{ ref('mart_product_performance') }}

WHERE revenue < 0