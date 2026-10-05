-- Business test:
-- Revenue should never be negative.

SELECT
    window_start,
    window_end,
    revenue

FROM {{ ref('mart_hourly_performance') }}

WHERE revenue < 0