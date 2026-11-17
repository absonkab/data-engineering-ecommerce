-- Intermediate model for user-level e-commerce analytics.
--
-- Business grain: one row per user and one-hour time window.
--
-- This model enriches the Gold metrics with additional KPIs
-- to analyze user activity and purchasing behavior.

SELECT

    -- User and time dimensions
    user_id,
    window_start,
    window_end,

    -- Original business metrics
    total_events,
    views,
    add_to_carts,
    purchases,
    revenue,
    unique_products,

    -- Purchase events as a proportion of all user events.
    -- This is an event-based rate, not a visitor conversion rate.
    purchase_rate,

    -- Average revenue generated per purchase event.
    CASE
        WHEN purchases > 0
        THEN revenue / purchases
        ELSE 0.0
    END AS revenue_per_purchase,

    -- Average number of events generated per distinct product.
    CASE
        WHEN unique_products > 0
        THEN total_events::FLOAT / unique_products
        ELSE 0.0
    END AS events_per_product,

    -- Add-to-cart events as a proportion of view events.
    CASE
        WHEN views > 0
        THEN add_to_carts::FLOAT / views
        ELSE 0.0
    END AS view_to_cart_rate,

    -- Purchase events as a proportion of view events.
    CASE
        WHEN views > 0
        THEN purchases::FLOAT / views
        ELSE 0.0
    END AS view_to_purchase_rate,

    -- Purchase events as a proportion of add-to-cart events.
    CASE
        WHEN add_to_carts > 0
        THEN purchases::FLOAT / add_to_carts
        ELSE 0.0
    END AS cart_to_purchase_rate

FROM {{ ref('stg_user_metrics') }}