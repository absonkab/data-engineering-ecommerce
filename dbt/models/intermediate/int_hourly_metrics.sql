-- Intermediate model for hourly ecommerce analytics.
--
-- This model builds on the staging layer and prepares
-- reusable hourly metrics for downstream marts.
--
-- Business grain:
-- one row per time window.
--
-- Grain:
-- (window_start, window_end)

SELECT
    window_start,
    window_end,

    -- Event activity
    total_events,
    views,
    add_to_carts,
    purchases,

    -- Revenue
    revenue,

    -- User activity
    unique_users,

    -- Purchase rate.
    --
    -- This represents the proportion of all events
    -- that are purchase events.
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
    END AS revenue_per_purchase

FROM {{ ref('stg_hourly_metrics') }}