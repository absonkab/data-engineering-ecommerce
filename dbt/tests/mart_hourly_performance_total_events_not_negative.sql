-- Business test:
-- The total number of events cannot be negative.

SELECT
    window_start,
    window_end,
    total_events

FROM {{ ref('mart_hourly_performance') }}

WHERE total_events < 0