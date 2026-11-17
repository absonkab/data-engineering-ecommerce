-- Test: Verify that business metrics are never negative
--
-- Business rule:
-- All event counts, distinct counts, revenues and calculated
-- KPIs must be greater than or equal to zero
--
-- Models covered:
--   - int_hourly_metrics
--   - int_product_metrics
--   - int_user_metrics

WITH metric_checks AS (

    -- HOURLY METRICS
    -- Business grain: one row per hourly window

    SELECT
        'int_hourly_metrics' AS model_name,
        TO_VARCHAR(window_start) AS record_identifier,
        window_start,
        metric_name,
        metric_value

    FROM int_hourly_metrics

    UNPIVOT (
        metric_value FOR metric_name IN (
            total_events,
            views,
            add_to_carts,
            purchases,
            revenue,
            unique_users,
            purchase_event_rate,
            view_to_cart_rate,
            view_to_purchase_rate,
            revenue_per_purchase,
            events_per_user
        )
    )

    UNION ALL

    -- PRODUCT METRICS
    -- Business grain: one row per product and hourly window

    SELECT
        'int_product_metrics' AS model_name,
        TO_VARCHAR(product_id) AS record_identifier,
        window_start,
        metric_name,
        metric_value

    FROM int_product_metrics

    UNPIVOT (
        metric_value FOR metric_name IN (
            total_events,
            views,
            add_to_carts,
            purchases,
            revenue,
            unique_users,
            unique_viewers,
            unique_buyers,
            conversion_rate,
            purchase_event_rate,
            view_to_cart_rate,
            cart_to_purchase_rate,
            revenue_per_purchase,
            revenue_per_buyer,
            revenue_per_view,
            events_per_user
        )
    )

    UNION ALL

    -- USER METRICS
    -- Business grain: one row per user and hourly window

    SELECT
        'int_user_metrics' AS model_name,
        TO_VARCHAR(user_id) AS record_identifier,
        window_start,
        metric_name,
        metric_value

    FROM int_user_metrics

    UNPIVOT (
        metric_value FOR metric_name IN (
            total_events,
            views,
            add_to_carts,
            purchases,
            revenue,
            unique_products,
            purchase_rate,
            revenue_per_purchase,
            events_per_product,
            view_to_cart_rate,
            view_to_purchase_rate,
            cart_to_purchase_rate
        )
    )
)

-- Return only metrics containing negative values.
SELECT
    model_name,
    record_identifier,
    window_start,
    metric_name,
    metric_value

FROM metric_checks

WHERE metric_value < 0