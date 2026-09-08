"""
Central configuration for the data engineering pipeline.

This module contains configuration shared across the different
pipeline components.
"""

# -------------------------------------------------------------------
# Kafka configuration
# -------------------------------------------------------------------

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "ecommerce_events"


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