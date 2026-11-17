-- Intermediate model for hourly e-commerce analytics.
--
-- Business grain: one row per one-hour time window.
--
-- This model enriches the Staging metrics with additional
-- business KPIs to support performance analysis.

SELECT

    -- Time dimensions
    window_start,
    window_end,

    -- Raw business metrics
    total_events,
    views,
    add_to_carts,
    purchases,
    revenue,
    unique_users,

    -- Purchase events as a proportion of all events.
    -- This is an event-based rate, not a visitor conversion rate.
    CASE
        WHEN total_events > 0
        THEN purchases::FLOAT / total_events
        ELSE 0.0
    END AS purchase_event_rate,

    -- Add-to-cart events as a proportion of views.
    CASE
        WHEN views > 0
        THEN add_to_carts::FLOAT / views
        ELSE 0.0
    END AS view_to_cart_rate,

    -- Purchase events as a proportion of views.
    CASE
        WHEN views > 0
        THEN purchases::FLOAT / views
        ELSE 0.0
    END AS view_to_purchase_rate,

    -- Average revenue generated per purchase event.
    CASE
        WHEN purchases > 0
        THEN revenue / purchases
        ELSE 0.0
    END AS revenue_per_purchase

    -- Average number of events per estimated unique user
    -- within the hourly time window.
    CASE
        WHEN unique_users > 0
        THEN total_events::FLOAT / unique_users
        ELSE 0.0
    END AS events_per_user

FROM {{ ref('stg_hourly_metrics') }}