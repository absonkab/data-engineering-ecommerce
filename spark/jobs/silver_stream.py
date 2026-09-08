"""
Silver streaming job.

Objective:
    Reads events from the Bronze layer and produces two outputs:
    1. SILVER: valid, cleaned, and deduplicated events
    2. QUARANTINE: invalid events with by their rejection reason

    
                                                  Valid events > watermark + deduplication > Silver
Architecture : Bronze > Cleaning / Normalization < 
                                                  Invalid events > rejection_reason > Quarantine
                   
"""


from pyspark.sql import functions as F
from utils.spark_session import create_spark_session
from schemas.ecommerce_schema import BRONZE_SCHEMA


# Spark Session

spark = create_spark_session("ecommerce-silver")


# Data Lake paths

BRONZE_PATH = "/lake/bronze"
SILVER_PATH = "/lake/silver"
QUARANTINE_PATH = "/lake/quarantine"
SILVER_CHECKPOINT = "/lake/checkpoints/silver"
QUARANTINE_CHECKPOINT = "/lake/checkpoints/quarantine"


# Read Bronze

bronze_df = (
    spark.readStream
    .format("parquet")
    .schema(BRONZE_SCHEMA)
    .load(BRONZE_PATH)
)


# Data Cleaning / Normalization before applying data quality rules.

cleaned_df = (
    bronze_df
    .withColumn(
        "event_id",
        F.trim(F.col("event_id")) # Removal of any spaces. 
    )
    .withColumn(
        "event_type",
        F.lower(F.trim(F.col("event_type"))) # Standardization: "VIEW" -> "view", "Purchase " -> "purchase"
    )
    .withColumn(
        "user_id",
        F.col("user_id").cast("integer") # Convert IDs to integer.
    )
    .withColumn(
        "product_id",
        F.col("product_id").cast("integer")
    )
    .withColumn(
        "price",
        F.col("price").cast("double") # Convert Price to type double
    )
    # Event time
    .withColumn(
        "timestamp",
        F.col("timestamp").cast("timestamp")
    )
    # Kafka ingestion time
    .withColumn(
        "kafka_timestamp",
        F.col("kafka_timestamp").cast("timestamp")
    )
)


# Data Quality rules
#
# An event is valid if :
#   - event_id exists
#   - user_id > 0
#   - product_id > 0
#   - event_type is known
#   - timestamp exists
#   - view/add_to_cart : price can be NULL
#   - purchase : price must be specified & > 0
#

valid_condition = (
    F.col("event_id").isNotNull() & (F.col("event_id") != "")

    & F.col("user_id").isNotNull() & (F.col("user_id") > 0)

    & F.col("product_id").isNotNull() & (F.col("product_id") > 0)

    & F.col("event_type").isin(
        "view",
        "add_to_cart",
        "purchase"
    )

    & F.col("timestamp").isNotNull()

    & (
        # view / add_to_cart: price can be NULL
        (
            F.col("event_type").isin(
                "view",
                "add_to_cart"
            )
            & F.col("price").isNull()
        )

        | #or

        # purchase: price required &  > 0
        (
            (F.col("event_type") == "purchase")
            & F.col("price").isNotNull()
            & (F.col("price") > 0)
        )
    )
)


# VALID EVENTS
# Filter first valid events.
# then apply:
#     watermark = 10 minutes, based on event time "timestamp".
#     deduplication = event_id
# This allows Spark to handle late-arriving events while enabling the gradual cleanup of the state used for deduplication.#

silver_df = (
    cleaned_df
    .filter(valid_condition)
    .withWatermark(
        "timestamp",
        "10 minutes"
    )
    .dropDuplicates(
        ["event_id"]
    )
)


# INVALID EVENTS / QUARANTINE
# Invalid events are NOT deleted. A "rejection_reason" column is added to indicate why the event was rejected.

quarantine_df = (
    cleaned_df
    .filter(~valid_condition)
    .withColumn(
        "rejection_reason",
        F.when(
            F.col("event_id").isNull()
            | (F.col("event_id") == ""),
            "missing_event_id"
        )
        .when(
            F.col("user_id").isNull()
            | (F.col("user_id") <= 0),
            "invalid_user_id"
        )
        .when(
            F.col("product_id").isNull()
            | (F.col("product_id") <= 0),
            "invalid_product_id"
        )
        .when(
            ~F.col("event_type").isin(
                "view",
                "add_to_cart",
                "purchase"
            ),
            "invalid_event_type"
        )
        .when(
            F.col("timestamp").isNull(),
            "missing_timestamp"
        )
        .when(
            (F.col("event_type") == "purchase")
            & (
                F.col("price").isNull()
                | (F.col("price") <= 0)
            ),
            "invalid_purchase_price"
        )
        .otherwise(
            "unknown_validation_error"
        )
    )
)


# Silver output
# append: Each micro-batch adds the new valid events.

silver_query = (
    silver_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        SILVER_PATH
    )
    .option(
        "checkpointLocation",
        SILVER_CHECKPOINT
    )
    .start()
)


# Quarantine output
# Invalid events are written separately. An independent Silver checkpoint is used.

quarantine_query = (
    quarantine_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        QUARANTINE_PATH
    )
    .option(
        "checkpointLocation",
        QUARANTINE_CHECKPOINT
    )
    .start()
)


# Keep both streaming queries alive

spark.streams.awaitAnyTermination()