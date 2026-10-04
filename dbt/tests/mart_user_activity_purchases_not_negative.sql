-- Business test:
-- The number of purchases cannot be negative.

SELECT
    window_start,
    window_end,
    user_id,
    purchases

FROM {{ ref('mart_user_activity') }}

WHERE purchases < 0