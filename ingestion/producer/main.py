# ingestion/producer/main.py

"""
Kafka producer entry point.
Orchestrates event generation and dispatch.
"""

import time
import random
import logging
from config.config import KAFKA_TOPIC, MIN_DELAY, MAX_DELAY
from producer.generator import generate_event
from producer.producer import create_producer, send_event

logger = logging.getLogger(__name__)


def run():
    """
    Main event-production loop.
    """
    producer = create_producer()

    try:
        while True:
            # Generate an event
            event = generate_event()

            # Send to Kafka
            send_event(producer, KAFKA_TOPIC, event)

            # Simulate a realistic (non-constant) flow.
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    except KeyboardInterrupt:
        logger.info("Stopping producer...")

    finally:
        # flush + close
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()