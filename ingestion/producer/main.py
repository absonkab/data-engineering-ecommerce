# ingestion/producer/main.py

"""
Kafka producer entry point.
Orchestrates event generation and dispatch.
"""

import time
import random
import logging
from producer.generator import generate_event
from producer.producer import create_producer, send_event
from config.config import (
    KAFKA_TOPIC,
    MIN_DELAY,
    MAX_DELAY,
    ANOMALY_PROBABILITY,
    ANOMALY_TYPES,
)

logger = logging.getLogger(__name__)

def should_duplicate():
    """
    Decide whether the current event should be sent twice.

    The duplicate probability is calculated from the global
    anomaly probability and the configured duplicate ratio.
    """

    duplicate_probability = (
        ANOMALY_PROBABILITY
        * ANOMALY_TYPES["duplicate"]
    )

    return random.random() < duplicate_probability


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

            # Simulate duplicate events
            if should_duplicate():
                logger.warning(
                    f"Simulating duplicate event: {event['event_id']}"
                )
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