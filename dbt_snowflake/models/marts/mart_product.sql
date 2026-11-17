-- Mart model for product-level e-commerce analytics.
--
-- Business grain: one row per product and one-hour time window.
--
-- This model is designed for BI tools such as Power BI.
-- It exposes product performance and purchasing KPIs.

SELECT

    -- Product and time dimensions
    product_id,
    window_start,
    window_end,

    -- Overall product activity
    total_events,
    unique_users,

    -- Product engagement
    views,
    unique_viewers,
    add_to_carts,

    -- Purchasing activity
    purchases,
    unique_buyers,
    conversion_rate,
    purchase_event_rate,

    -- Revenue performance
    revenue,
    revenue_per_purchase,
    revenue_per_buyer,
    revenue_per_view,

    -- Additional business indicators
    view_to_cart_rate,
    cart_to_purchase_rate,
    events_per_user

FROM {{ ref('int_product_metrics') }}