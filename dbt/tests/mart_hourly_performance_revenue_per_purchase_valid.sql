-- Business test:
-- Average revenue per purchase cannot be negative.

SELECT
    window_start,
    window_end,
    revenue_per_purchase

FROM {{ ref('mart_hourly_performance') }}

WHERE revenue_per_purchase < 0