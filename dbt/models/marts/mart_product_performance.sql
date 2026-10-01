-- Mart model for product performance analytics.
--
-- This model exposes product-level KPIs in a format
-- designed for dashboards and analytical consumption.

SELECT
    window_start,
    window_end,
    product_id,

    -- Activity
    total_events,
    views,
    add_to_carts,
    purchases,

    -- Revenue
    revenue,
    revenue_per_purchase,

    -- User engagement
    unique_users,
    unique_viewers,
    unique_buyers,
    events_per_user,

    -- Conversion metrics
    conversion_rate,
    purchase_rate

FROM {{ ref('int_product_metrics') }}