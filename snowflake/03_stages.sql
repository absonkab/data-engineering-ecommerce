USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;
USE WAREHOUSE ECOMMERCE_WH;

-- Create an internal stage
CREATE STAGE IF NOT EXISTS ECOMMERCE_RAW_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Show stages
SHOW STAGES;

-- List files present in stage
LIST @ECOMMERCE_RAW_STAGE;
LIST @ECOMMERCE_RAW_STAGE/hourly_metrics/;
LIST @ECOMMERCE_RAW_STAGE/product_metrics/;
LIST @ECOMMERCE_RAW_STAGE/user_metrics/;

-- show hourly dataset schema
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@ECOMMERCE_RAW_STAGE/hourly_metrics/',
        FILE_FORMAT => 'ECOMMERCE_PARQUET_FORMAT'
    )
);

-- show product dataset schema
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@ECOMMERCE_RAW_STAGE/product_metrics/',
        FILE_FORMAT => 'ECOMMERCE_PARQUET_FORMAT'
    )
);

-- show user dataset schema
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@ECOMMERCE_RAW_STAGE/user_metrics/',
        FILE_FORMAT => 'ECOMMERCE_PARQUET_FORMAT'
    )
);