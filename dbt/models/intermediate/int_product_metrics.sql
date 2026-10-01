-- Intermediate model for product-level ecommerce analytics.
--
-- This model builds on the staging layer and prepares
-- reusable product-level metrics for downstream marts.

SELECT
    window_start,
    window_end,
    product_id,

    -- Event volume
    total_events,
    views,
    add_to_carts,
    purchases,

    -- Revenue
    revenue,

    -- User metrics
    unique_users,
    unique_viewers,
    unique_buyers,

    -- Existing conversion metric
    conversion_rate,

    -- Number of purchases relative to all events.
    CASE
        WHEN total_events > 0
        THEN purchases::NUMERIC / total_events
        ELSE 0
    END AS purchase_rate,

    -- Average revenue generated per purchase.
    CASE
        WHEN purchases > 0
        THEN revenue / purchases
        ELSE 0
    END AS revenue_per_purchase,

    -- Average number of events generated per user.
    CASE
        WHEN unique_users > 0
        THEN total_events::NUMERIC / unique_users
        ELSE 0
    END AS events_per_user

FROM {{ ref('stg_product_metrics') }}