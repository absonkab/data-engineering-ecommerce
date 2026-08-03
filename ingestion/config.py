# ingestion/config.py

"""
Centralise toute la configuration du projet.
Pour éviter les valeurs hardcodées partout.
"""

KAFKA_BROKER = "kafka:9092"
TOPIC = "ecommerce_events"

# Simulation config
EVENT_TYPES = ["view", "add_to_cart", "purchase"]

# Débit du flux
MIN_DELAY = 0.5
MAX_DELAY = 2