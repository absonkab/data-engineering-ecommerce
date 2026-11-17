-- Mart model for user-level e-commerce analytics.
--
-- Business grain: one row per user and one-hour time window.
--
-- This model is designed for BI tools such as Power BI.
-- It exposes user activity and purchasing behavior KPIs.

SELECT

    -- User and time dimensions
    user_id,
    window_start,
    window_end,

    -- Overall user activity
    total_events,
    unique_products,
    events_per_product,

    -- User engagement
    views,
    add_to_carts,

    -- Purchasing activity
    purchases,
    purchase_rate,

    -- Revenue performance
    revenue,
    revenue_per_purchase,

    -- User behavior indicators
    view_to_cart_rate,
    view_to_purchase_rate,
    cart_to_purchase_rate

FROM {{ ref('int_user_metrics') }}