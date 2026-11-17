-- Test: Verify product-level uniqueness per hourly window.
--
-- Business rule:
-- Each product must appear only once within a given hourly window.
--
-- Expected result:
-- The query must return zero rows.
-- Any returned row represents a duplicate product/window combination.

SELECT
    product_id,
    window_start,

    -- Count the number of occurrences for each product/window pair.
    COUNT(*) AS duplicate_count

FROM {{ ref('int_product_metrics') }}

-- Group records by their business key.
GROUP BY
    product_id,
    window_start

-- Return only combinations appearing more than once.
HAVING COUNT(*) > 1