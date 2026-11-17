-- Intermediate model for product-level e-commerce analytics.
--
-- Business grain: one row per product and one-hour time window.
--
-- This model enriches the Gold metrics with additional KPIs
-- to analyze product performance and purchasing behavior.

SELECT

    -- Product and time dimensions
    product_id,
    window_start,
    window_end,

    -- Original business metrics
    total_events,
    views,
    add_to_carts,
    purchases,
    revenue,
    unique_users,
    unique_viewers,
    unique_buyers,

    -- Conversion rate calculated by Spark:
    -- approximate unique buyers / approximate unique viewers.
    -- This is a user-based conversion rate.
    conversion_rate,

    -- Purchase events as a proportion of all product events.
    CASE
        WHEN total_events > 0
        THEN purchases::FLOAT / total_events
        ELSE 0.0
    END AS purchase_event_rate,

    -- Add-to-cart events as a proportion of view events.
    CASE
        WHEN views > 0
        THEN add_to_carts::FLOAT / views
        ELSE 0.0
    END AS view_to_cart_rate,

    -- Purchase events as a proportion of add-to-cart events.
    CASE
        WHEN add_to_carts > 0
        THEN purchases::FLOAT / add_to_carts
        ELSE 0.0
    END AS cart_to_purchase_rate,

    -- Average revenue generated per purchase event.
    CASE
        WHEN purchases > 0
        THEN revenue / purchases
        ELSE 0.0
    END AS revenue_per_purchase,

    -- Average revenue generated per estimated unique buyer.
    CASE
        WHEN unique_buyers > 0
        THEN revenue / unique_buyers
        ELSE 0.0
    END AS revenue_per_buyer,

    -- Revenue generated relative to the number of view events.
    CASE
        WHEN views > 0
        THEN revenue / views
        ELSE 0.0
    END AS revenue_per_view

    -- Average number of events generated per unique user.
    -- unique_users is an approximate distinct count calculated by Spark.
    CASE
        WHEN unique_users > 0
        THEN total_events::FLOAT / unique_users
        ELSE 0.0
    END AS events_per_user

FROM {{ ref('stg_product_metrics') }}