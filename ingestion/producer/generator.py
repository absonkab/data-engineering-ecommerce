# ingestion/producer/generator.py

"""
Generation of simulated e-commerce events to produce realistic data.
"""

import uuid
import random
from datetime import datetime
from config.config import EVENT_TYPES


def generate_event():
    """
    Generate an user simulated event.

    Returns:
        dict: structured event
    """

    event_type = random.choice(EVENT_TYPES)

    event = {
        # Unique ID for traceability
        "event_id": str(uuid.uuid4()),

        # User simulation
        "user_id": random.randint(1, 1000),

        # Product simulation
        "product_id": random.randint(1, 100),

        # event type
        "event_type": event_type,

        # Price for purchase only
        "price": round(random.uniform(5, 500), 2) if event_type == "purchase" else None,

        # Timestamp ISO
        "timestamp": datetime.utcnow().isoformat()
    }

    return event