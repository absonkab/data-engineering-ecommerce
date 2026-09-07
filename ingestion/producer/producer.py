# ingestion/producer/producer.py

"""
Kafka producer management.
Responsible for connection and message sending.
"""

import json
import logging
from kafka import KafkaProducer
from config.config import KAFKA_BROKER


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def create_producer():
    """
    Initializes the Kafka producer.

    Returns:
        KafkaProducer
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,

            # Automatic JSON serialization
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),

            # Retry on failure
            retries=5
        )

        logger.info("Kafka producer connected")
        return producer

    except Exception as e:
        logger.error(f"Kafka connection error: {e}")
        raise


def send_event(producer, topic, event):
    """
    Send an event in Kafka.

    Args:
        producer: Kafka instance
        topic (str): topic name
        event (dict): event to send
    """
    try:
        producer.send(topic, value=event)
        logger.info(f"Event sent: {event}")

    except Exception as e:
        logger.error(f"Error sending event: {e}")