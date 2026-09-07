"""
First Spark Structured Streaming job.

Objective :
    Read e-commerce events from Kafka,
    transform raw JSON into structured data,
    then persist them into the Bronze layer of the Data Lake.

Architecture : Kafka > Spark Structured Streaming > JSON parsing > Bronze(/lake/bronze)

The Spark checkpoint is stored separately : /lake/checkpoints/bronze then Spark can memorize the progression in the flow.
"""

from pyspark.sql.functions import col, from_json

from utils.spark_session import create_spark_session
from config.config import KAFKA_BROKER, KAFKA_TOPIC
from schemas.ecommerce_schema import EVENT_SCHEMA


# Spark Session: Use of our centralized function to create Spark Session.

spark = create_spark_session("EcommerceKafkaStreaming")


# Kafka flow reading

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest") # Read from newest data
    .load()
)


# Payload JSON extraction
# Convert value column returned by kafka to String before using from_json().
# Conserve kafka metadata for traceability and for debugging

json_df = kafka_df.select(
    # Payload JSON from our producer
    col("value").cast("string").alias("json_value"),

    # Kafka Metadata
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
)

# Parse JSON into Spark structure using the schema defined in spark/schemas/ecommerce_schema.py

parsed_df = (
    json_df
    .select(
        from_json(
            col("json_value"),
            EVENT_SCHEMA
        ).alias("data"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
    )
    .select(
        # business data
        "data.*",
        # Kafka metadata
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
)

# Write into the Bronze layer
# Format: Parquet
# Destination : /lake/bronze

query = (
    parsed_df
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", "/lake/bronze")
    .option(
        "checkpointLocation",
        "/lake/checkpoints/bronze"
    )
    .start()
)


# Keep the job active

query.awaitTermination() # awaitTermination() blocks the main process so that the Spark Streaming application continues to run.