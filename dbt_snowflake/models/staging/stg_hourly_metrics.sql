-- Staging model for hourly e-commerce metrics.
-- Grain: one row per one-hour time window.
-- This model provides a clean SQL interface over the RAW table.

SELECT
    WINDOW_START,
    WINDOW_END,
    TOTAL_EVENTS,
    VIEWS,
    ADD_TO_CARTS,
    PURCHASES,
    REVENUE,
    UNIQUE_USERS

FROM {{ source('raw', 'RAW_HOURLY_METRICS') }}