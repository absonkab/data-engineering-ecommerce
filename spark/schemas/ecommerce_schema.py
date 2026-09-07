"""
E-commerce Event Schema

This schema defines the JSON structure of the events generated
by the Kafka producer.

It is used by Spark Structured Streaming to convert
raw JSON input into strongly typed data.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


# -------------------------------------------------------------------
# E-commerce Event Schema
# -------------------------------------------------------------------
#
# Example of JSON from Kafka :
#
# {
#     "event_id": "940b6e87-f8df-48c6-b4c6-09286e17b985",
#     "user_id": 42,
#     "product_id": 105,
#     "event_type": "purchase",
#     "price": 79.99,
#     "timestamp": "2026-09-07T09:30:15"
# }
#
# -------------------------------------------------------------------

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])