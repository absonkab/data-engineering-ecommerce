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