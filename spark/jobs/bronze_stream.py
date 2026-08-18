"""
Premier job Spark Structured Streaming.

Objectif : Transformer le JSON brut Kafka en données structurées et de les persister dans la Bronze layer

"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)
from pyspark.sql.functions import col, from_json

from utils.spark_session import create_spark_session
from config.config import KAFKA_BROKER, KAFKA_TOPIC

# -------------------------------------------------------------------
# 1. Spark session
# -------------------------------------------------------------------

spark = create_spark_session("EcommerceKafkaStreaming")


# -------------------------------------------------------------------
# 2. Define the schema of incoming Kafka events
# -------------------------------------------------------------------

event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])


# -------------------------------------------------------------------
# 3. Read events from Kafka
# -------------------------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


# -------------------------------------------------------------------
# 4. Parse the JSON payload
# -------------------------------------------------------------------

parsed_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            event_schema
        ).alias("data")
    )
    .select("data.*")
)


# -------------------------------------------------------------------
# 5. Display the structured events
#
# This is only a validation step.
# The Bronze persistence layer will be added next.
# -------------------------------------------------------------------

query = (
    parsed_df
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .option("checkpointLocation", "/opt/spark/checkpoints/bronze")
    .start()
)


# -------------------------------------------------------------------
# 6. Keep the streaming application running
# -------------------------------------------------------------------

query.awaitTermination()