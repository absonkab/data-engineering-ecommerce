# ingestion/producer/generator.py

"""
Génération des événements e-commerce simulés.
Responsabilité unique : produire des données réalistes.
"""

import uuid
import random
from datetime import datetime
from producer.config import EVENT_TYPES


def generate_event():
    """
    Génère un événement utilisateur simulé.

    Returns:
        dict: événement structuré
    """

    event_type = random.choice(EVENT_TYPES)

    event = {
        # ID unique pour la traçabilité
        "event_id": str(uuid.uuid4()),

        # Simulation utilisateur
        "user_id": random.randint(1, 1000),

        # Simulation produit
        "product_id": random.randint(1, 100),

        # Type d'événement
        "event_type": event_type,

        # Prix uniquement pour purchase
        "price": round(random.uniform(5, 500), 2) if event_type == "purchase" else None,

        # Timestamp ISO
        "timestamp": datetime.utcnow().isoformat()
    }

    return event