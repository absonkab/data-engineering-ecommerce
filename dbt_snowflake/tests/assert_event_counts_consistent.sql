-- Test: Verify consistency between event counters and total events.
--
-- Business rules:
--   - views must not exceed total_events.
--   - add_to_carts must not exceed total_events.
--   - purchases must not exceed total_events.
--
-- Models covered:
--   - int_hourly_metrics
--   - int_product_metrics
--   - int_user_metrics

WITH event_count_violations AS (

    -- HOURLY METRICS

    SELECT
        'int_hourly_metrics' AS model_name,
        TO_VARCHAR(window_start) AS record_identifier,
        window_start,
        'views' AS metric_name,
        views AS actual_value,
        total_events AS expected_maximum

    FROM {{ ref('int_hourly_metrics') }}

    WHERE views > total_events

    UNION ALL

    SELECT
        'int_hourly_metrics',
        TO_VARCHAR(window_start),
        window_start,
        'add_to_carts',
        add_to_carts,
        total_events

    FROM {{ ref('int_hourly_metrics') }}

    WHERE add_to_carts > total_events

    UNION ALL

    SELECT
        'int_hourly_metrics',
        TO_VARCHAR(window_start),
        window_start,
        'purchases',
        purchases,
        total_events

    FROM {{ ref('int_hourly_metrics') }}

    WHERE purchases > total_events

    UNION ALL

    -- PRODUCT METRICS

    SELECT
        'int_product_metrics',
        TO_VARCHAR(product_id),
        window_start,
        'views',
        views,
        total_events

    FROM {{ ref('int_product_metrics') }}

    WHERE views > total_events

    UNION ALL

    SELECT
        'int_product_metrics',
        TO_VARCHAR(product_id),
        window_start,
        'add_to_carts',
        add_to_carts,
        total_events

    FROM {{ ref('int_product_metrics') }}

    WHERE add_to_carts > total_events

    UNION ALL

    SELECT
        'int_product_metrics',
        TO_VARCHAR(product_id),
        window_start,
        'purchases',
        purchases,
        total_events

    FROM {{ ref('int_product_metrics') }}

    WHERE purchases > total_events

    UNION ALL

    -- USER METRICS

    SELECT
        'int_user_metrics',
        TO_VARCHAR(user_id),
        window_start,
        'views',
        views,
        total_events

    FROM {{ ref('int_user_metrics') }}

    WHERE views > total_events

    UNION ALL

    SELECT
        'int_user_metrics',
        TO_VARCHAR(user_id),
        window_start,
        'add_to_carts',
        add_to_carts,
        total_events

    FROM {{ ref('int_user_metrics') }}

    WHERE add_to_carts > total_events

    UNION ALL

    SELECT
        'int_user_metrics',
        TO_VARCHAR(user_id),
        window_start,
        'purchases',
        purchases,
        total_events

    FROM {{ ref('int_user_metrics') }}

    WHERE purchases > total_events

)

-- Return only records violating the business rules.
SELECT
    model_name,
    record_identifier,
    window_start,
    metric_name,
    actual_value,
    expected_maximum

FROM event_count_violations