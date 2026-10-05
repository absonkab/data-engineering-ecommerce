-- Business test:
-- There must be exactly one row per time window.
--
-- Business grain:
-- (window_start, window_end)

SELECT
    window_start,
    window_end,
    COUNT(*) AS row_count

FROM {{ ref('mart_hourly_performance') }}

GROUP BY
    window_start,
    window_end

HAVING COUNT(*) > 1