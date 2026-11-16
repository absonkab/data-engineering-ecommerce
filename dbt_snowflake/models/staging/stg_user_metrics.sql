-- Staging model for user-level e-commerce metrics.
-- Grain: one row per user and one-hour time window.

SELECT
    USER_ID,
    WINDOW_START,
    WINDOW_END,
    TOTAL_EVENTS,
    VIEWS,
    ADD_TO_CARTS,
    PURCHASES,
    REVENUE,
    UNIQUE_PRODUCTS,
    PURCHASE_RATE

FROM {{ source('raw', 'RAW_USER_METRICS') }}