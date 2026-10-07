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
# Configuration Gold / Serving
# ============================================================

GOLD_HOURLY_METRICS_TABLE = "gold_hourly_metrics"
GOLD_PRODUCT_METRICS_TABLE = "gold_product_metrics"
GOLD_USER_METRICS_TABLE = "gold_user_metrics"