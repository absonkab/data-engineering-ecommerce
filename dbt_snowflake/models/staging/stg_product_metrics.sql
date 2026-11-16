-- Staging model for product-level e-commerce metrics.
-- Grain: one row per product and one-hour time window.

SELECT
    PRODUCT_ID,
    WINDOW_START,
    WINDOW_END,
    TOTAL_EVENTS,
    VIEWS,
    ADD_TO_CARTS,
    PURCHASES,
    REVENUE,
    UNIQUE_USERS,
    UNIQUE_VIEWERS,
    UNIQUE_BUYERS,
    CONVERSION_RATE

FROM {{ source('raw', 'RAW_PRODUCT_METRICS') }}