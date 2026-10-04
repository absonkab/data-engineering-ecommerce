-- Business test:
-- Revenue should never be negative.

SELECT
    window_start,
    window_end,
    user_id,
    revenue

FROM {{ ref('mart_user_activity') }}

WHERE revenue < 0