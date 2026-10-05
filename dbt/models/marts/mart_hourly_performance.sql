-- Mart model for hourly ecommerce performance.
--
-- This model exposes hourly KPIs in a format
-- designed for dashboards and analytical consumption.
--
-- Business grain:
-- one row per time window.

SELECT
    window_start,
    window_end,

    -- Activity
    total_events,
    views,
    add_to_carts,
    purchases,

    -- Revenue
    revenue,
    revenue_per_purchase,

    -- User activity
    unique_users,

    -- Purchase behavior
    purchase_rate

FROM {{ ref('int_hourly_metrics') }}