-- Business test:
-- The number of purchases cannot be negative.

SELECT
    window_start,
    window_end,
    purchases

FROM {{ ref('mart_hourly_performance') }}

WHERE purchases < 0