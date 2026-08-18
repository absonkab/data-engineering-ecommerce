# ingestion/producer/main.py

"""
Point d'entrée du producer Kafka.
Orchestre la génération + envoi des événements.
"""

import time
import random
import logging
from producer.config import TOPIC, MIN_DELAY, MAX_DELAY
from producer.generator import generate_event
from producer.producer import create_producer, send_event

logger = logging.getLogger(__name__)


def run():
    """
    Boucle principale de production d'événements.
    """
    producer = create_producer()

    try:
        while True:
            # Générer un événement
            event = generate_event()

            # Envoyer à Kafka
            send_event(producer, TOPIC, event)

            # Simuler un flux réaliste (pas constant)
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    except KeyboardInterrupt:
        logger.info("Stopping producer...")

    finally:
        # flush + close
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()