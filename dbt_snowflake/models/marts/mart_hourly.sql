-- Mart model for global hourly e-commerce analytics.
--
-- Business grain: one row per one-hour time window.
--
-- This model is designed for BI tools such as Power BI.
-- It exposes business metrics in a clear and analysis-ready format.

SELECT

    -- Time dimensions
    window_start,
    window_end,

    -- Overall activity
    total_events,
    unique_users,

    -- E-commerce event metrics
    views,
    add_to_carts,
    purchases,

    -- Revenue
    revenue,
    revenue_per_purchase,

    -- Business performance indicators
    purchase_event_rate,
    view_to_cart_rate,
    view_to_purchase_rate,

    -- Average activity per estimated unique user
    events_per_user

FROM {{ ref('int_hourly_metrics') }}