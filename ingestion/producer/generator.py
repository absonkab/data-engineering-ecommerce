# ingestion/producer/generator.py

"""
Generation of simulated e-commerce events to produce realistic data.
"""

import uuid
import random
from datetime import datetime, timedelta
from config.config import (
    EVENT_TYPES,
    ANOMALY_PROBABILITY,
    ANOMALY_TYPES,
    LATE_EVENT_MINUTES,
)


def generate_event():
    """
    Generate a simulated e-commerce event.

    The generator can intentionally introduce data quality issues
    in order to test the Silver validation and Quarantine layers.

    Returns:
        dict: simulated event
    """

    event_type = random.choice(EVENT_TYPES)

    # ---------------------------------------------------------------
    # Generate a normal event first
    # ---------------------------------------------------------------

    event = {
        # Unique ID for traceability
        "event_id": str(uuid.uuid4()),

        # User simulation
        "user_id": random.randint(1, 1000),

        # Product simulation
        "product_id": random.randint(1, 100),

        # Event type
        "event_type": event_type,

        # Price for purchase only
        "price": (
            round(random.uniform(5, 500), 2)
            if event_type == "purchase"
            else None
        ),

        # Event timestamp
        "timestamp": datetime.utcnow().isoformat()
    }

    # ---------------------------------------------------------------
    # Decide whether to introduce an anomaly
    # ---------------------------------------------------------------

    if random.random() < ANOMALY_PROBABILITY:

        anomaly_type = random.choices(
            population=list(ANOMALY_TYPES.keys()),
            weights=list(ANOMALY_TYPES.values()),
            k=1
        )[0]

        # Missing data

        if anomaly_type == "missing":

            field = random.choice([
                "event_id",
                "user_id",
                "product_id",
                "timestamp",
            ])

            event[field] = None

        # Invalid data

        elif anomaly_type == "invalid":

            invalid_case = random.choice([
                "user_id",
                "product_id",
                "event_type",
                "purchase_price",
            ])

            if invalid_case == "user_id":
                event["user_id"] = -random.randint(1, 100)

            elif invalid_case == "product_id":
                event["product_id"] = 0

            elif invalid_case == "event_type":
                event["event_type"] = "unknown"

            elif invalid_case == "purchase_price":
                event["event_type"] = "purchase"
                event["price"] = -10.0

        # Late event

        elif anomaly_type == "late":

            event_time = datetime.utcnow() - timedelta(
                minutes=LATE_EVENT_MINUTES
            )

            event["timestamp"] = event_time.isoformat()

        # Duplicate

        elif anomaly_type == "duplicate":
            # The event itself remains valid.
            # The duplication is handled by main.py, which will send the same event twice.
            pass

    return event