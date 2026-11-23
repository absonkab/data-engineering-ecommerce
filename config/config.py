"""
Central configuration for the data engineering pipeline.

This module contains configuration shared across the different
pipeline components.
"""

import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# -------------------------------------------------------------------
# Kafka configuration
# -------------------------------------------------------------------

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "ecommerce_events"


# -------------------------------------------------------------------
# Spark configuration
# -------------------------------------------------------------------
# The watermark allows Spark to limit the amount of state maintained for the streaming aggregation.
# Events that arrive very late (here more than 10 minutes after the current event-time progress) may no longer modify a window that Spark already considers closed.

WATERMARK_DELAY = "10 minutes"

# -------------------------------------------------------------------
# Event simulation configuration
# -------------------------------------------------------------------

EVENT_TYPES = [
    "view",
    "add_to_cart",
    "purchase"
]

MIN_DELAY = 0.5
MAX_DELAY = 2

# -------------------------------------------------------------------
# Data quality anomaly simulation
# -------------------------------------------------------------------

# Probability that a generated event will contain an anomaly.
ANOMALY_PROBABILITY = 0.20

# Probabilities used when an anomalous event is selected.
# The values should add up to 1.0.
ANOMALY_TYPES = {
    "missing": 0.30,
    "invalid": 0.30,
    "late": 0.20,
    "duplicate": 0.20,
}

# Delay applied to intentionally late events.
# This is greater than the 10-minute Spark watermark.
LATE_EVENT_MINUTES = 15

# -------------------------------------------------------------------
# Data Lake configuration
# -------------------------------------------------------------------

# Bronze paths
BRONZE_PATH = "/lake/bronze"
BRONZE_CHECKPOINT_PATH = "/lake/checkpoints/bronze"

# Silver / Quarantine paths
SILVER_PATH = "/lake/silver"
SILVER_CHECKPOINT_PATH = "/lake/checkpoints/silver"
QUARANTINE_PATH = "/lake/quarantine"
QUARANTINE_CHECKPOINT_PATH = "/lake/checkpoints/quarantine"

# Gold paths
GOLD_PATH = "/lake/gold"
GOLD_CHECKPOINT_PATH = "/lake/checkpoints/gold"

# Gold datasets produced by the existing pipeline
GOLD_DATASETS = [
    "hourly_metrics",
    "product_metrics",
    "user_metrics",
]

# Gold Paths for Snowflake export
SNOWFLAKE_EXPORT_PATH = "/lake/snowflake_export"

# Dataset configuration for Snowflake export

# Centralizes the source and destination paths of each dataset
# This avoids duplicating path definitions throughout the spark job

EXPORT_DATASETS = {
    "hourly_metrics": {
        "source": os.path.join(GOLD_PATH, "hourly_metrics"),
        "destination": os.path.join(SNOWFLAKE_EXPORT_PATH, "hourly_metrics")
    },
    "product_metrics": {
        "source": os.path.join(GOLD_PATH, "product_metrics"),
        "destination": os.path.join(SNOWFLAKE_EXPORT_PATH, "product_metrics"),
    },
    "user_metrics": {
        "source": os.path.join(GOLD_PATH, "user_metrics"),
        "destination": os.path.join(SNOWFLAKE_EXPORT_PATH, "user_metrics"),
    },
}

# ============================================================
# Configuration PostgreSQL
# ============================================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommerce")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# ============================================================
# Configuration Spark & Gold / Serving
# ============================================================

GOLD_HOURLY_METRICS_TABLE = "gold_hourly_metrics"
GOLD_PRODUCT_METRICS_TABLE = "gold_product_metrics"
GOLD_USER_METRICS_TABLE = "gold_user_metrics"

# Existing Spark job container
SPARK_CONTAINER = "spark-job"

# Spark Master REST API
SPARK_MASTER_URL = "http://spark:8080/json/"

# Maximum time to wait for an available Spark Worker
SPARK_WORKER_CHECK_TIMEOUT = 60

# Delay between two availability checks
SPARK_WORKER_CHECK_INTERVAL = 5

# Minimum resources required on an available Worker
MIN_FREE_CORES = 1
MIN_FREE_MEMORY_MB = 1024

# ============================================================
# Snowflake configuration
# ============================================================

# Existing Snowflake loader container
SNOWFLAKE_CONTAINER = "dbt-snowflake"

SNOWFLAKE_CONFIG = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    "role": os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": "ECOMMERCE_DB",
    "schema": "RAW",
}

STAGE_NAME = "ECOMMERCE_RAW_STAGE"
FILE_FORMAT_NAME = "ECOMMERCE_PARQUET_FORMAT"

# Dataset configuration
SNOWFLAKE_DATASETS = {
    "hourly_metrics": {
        "raw_table": "RAW_HOURLY_METRICS",
        "temp_table": "TMP_RAW_HOURLY_METRICS",
    },
    "product_metrics": {
        "raw_table": "RAW_PRODUCT_METRICS",
        "temp_table": "TMP_RAW_PRODUCT_METRICS",
    },
    "user_metrics": {
        "raw_table": "RAW_USER_METRICS",
        "temp_table": "TMP_RAW_USER_METRICS",
    },
}
