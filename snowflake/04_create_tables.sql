USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;
USE WAREHOUSE ECOMMERCE_WH;

-- Create hourly metrics table from parquet file in stage 
-- OBJECT_CONSTRUCT transforms each column definition into an object / ARRAY_AGG arranges the definitions into a table
-- USING TEMPLATE generate the table structure from ARRAY_AGG table
CREATE TABLE IF NOT EXISTS RAW_HOURLY_METRICS
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
           WITHIN GROUP (ORDER BY ORDER_ID)
    FROM TABLE(
        -- retrieve columns and their types
        INFER_SCHEMA(
            LOCATION => '@ECOMMERCE_RAW_STAGE/hourly_metrics/',
            FILE_FORMAT => 'ECOMMERCE_PARQUET_FORMAT'
        )
    )
);

-- Create product metrics table from parquet file in stage 
CREATE TABLE IF NOT EXISTS RAW_PRODUCT_METRICS
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
           WITHIN GROUP (ORDER BY ORDER_ID)
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@ECOMMERCE_RAW_STAGE/product_metrics/',
            FILE_FORMAT => 'ECOMMERCE_PARQUET_FORMAT'
        )
    )
);

-- Create user metrics table from parquet file in stage 
CREATE TABLE IF NOT EXISTS RAW_USER_METRICS
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
           WITHIN GROUP (ORDER BY ORDER_ID)
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@ECOMMERCE_RAW_STAGE/user_metrics/',
            FILE_FORMAT => 'ECOMMERCE_PARQUET_FORMAT'
        )
    )
);

-- check tables existence
DESC TABLE RAW_HOURLY_METRICS;
DESC TABLE RAW_PRODUCT_METRICS;
DESC TABLE RAW_USER_METRICS;