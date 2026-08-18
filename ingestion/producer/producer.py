# ingestion/producer/producer.py

"""
Gestion du producer Kafka.
Responsable de la connexion et de l'envoi des messages.
"""

import json
import logging
from kafka import KafkaProducer
from producer.config import KAFKA_BROKER


# Configuration du logging (important en prod)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def create_producer():
    """
    Initialise le producer Kafka.

    Returns:
        KafkaProducer
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,

            # Sérialisation JSON automatique
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),

            # Retry en cas d'échec (résilience)
            retries=5
        )

        logger.info("Kafka producer connected")
        return producer

    except Exception as e:
        logger.error(f"Kafka connection error: {e}")
        raise


def send_event(producer, topic, event):
    """
    Envoie un événement dans Kafka.

    Args:
        producer: instance Kafka
        topic (str): nom du topic
        event (dict): événement à envoyer
    """
    try:
        producer.send(topic, value=event)
        logger.info(f"Event sent: {event}")

    except Exception as e:
        logger.error(f"Error sending event: {e}")