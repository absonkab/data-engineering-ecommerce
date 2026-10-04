-- Business test:
-- Purchase rate must be between 0 and 1.

SELECT
    window_start,
    window_end,
    user_id,
    purchase_rate

FROM {{ ref('mart_user_activity') }}

WHERE purchase_rate < 0
   OR purchase_rate > 1