-- Business test:
-- The number of unique users cannot be negative.

SELECT
    window_start,
    window_end,
    unique_users

FROM {{ ref('mart_hourly_performance') }}

WHERE unique_users < 0