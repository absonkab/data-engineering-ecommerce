-- Business test:
-- Average revenue per purchase cannot be negative.

SELECT
    window_start,
    window_end,
    user_id,
    revenue_per_purchase

FROM {{ ref('mart_user_activity') }}

WHERE revenue_per_purchase < 0