-- Mart model for user activity analytics.
--
-- This model exposes user-level KPIs in a format
-- designed for dashboards and analytical consumption.
--
-- The model depends on the intermediate user model
-- through dbt's ref() function.
--
-- Business grain:
-- one row per user and one time window.

SELECT
    window_start,
    window_end,
    user_id,

    -- Activity
    total_events,
    views,
    add_to_carts,
    purchases,

    -- Revenue
    revenue,
    revenue_per_purchase,

    -- Product engagement
    unique_products,
    events_per_product,

    -- Conversion / purchase behavior
    purchase_rate

FROM {{ ref('int_user_metrics') }}