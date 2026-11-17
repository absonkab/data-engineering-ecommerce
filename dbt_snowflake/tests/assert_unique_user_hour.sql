-- Test: Verify user-level uniqueness per hourly window.
--
-- Business rule:
-- Each user must appear only once within a given hourly window.
--
-- Expected result:
-- The query must return zero rows.
-- Any returned row represents a duplicate user/window combination.

SELECT
    user_id,
    window_start,

    -- Count the number of occurrences for each user/window pair.
    COUNT(*) AS duplicate_count

FROM {{ ref('int_user_metrics') }}

-- Group records by their business key.
GROUP BY
    user_id,
    window_start

-- Return only combinations appearing more than once.
HAVING COUNT(*) > 1