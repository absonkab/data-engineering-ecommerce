-- Business test:
-- Each user must have exactly one row per time window.
--
-- Business grain:
-- (window_start, window_end, user_id)

SELECT
    window_start,
    window_end,
    user_id,
    COUNT(*) AS row_count

FROM {{ ref('mart_user_activity') }}

GROUP BY
    window_start,
    window_end,
    user_id

HAVING COUNT(*) > 1