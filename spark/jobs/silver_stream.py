"""
Silver streaming job.

Objective:
    Reads events from the Bronze layer and produces two outputs:
    1. SILVER: valid, cleaned, and deduplicated events
    2. QUARANTINE: invalid events with by their rejection reason

    
                                                  Valid events > watermark + deduplication > Silver
Architecture : Bronze > Cleaning / Normalization < 
                                                  Invalid events > rejection_reason > Quarantine

The Silver layer is the trusted source for downstream analytics and Gold aggregations.
"""


from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from utils.spark_session import create_spark_session
from schemas.ecommerce_schema import BRONZE_SCHEMA
from config.config import (
    BRONZE_PATH,
    SILVER_PATH,
    QUARANTINE_PATH,
    SILVER_CHECKPOINT_PATH,
    QUARANTINE_CHECKPOINT_PATH,
    WATERMARK_DELAY
)


# Data cleaning / normalization

def clean_events(bronze_df: DataFrame) -> DataFrame:
    """
    Clean and normalize Bronze events.

    The function:
        - removes unnecessary spaces from event_id,
        - normalizes event_type to lowercase,
        - converts IDs to integers,
        - converts price to double,
        - converts event timestamp to timestamp,
        - converts Kafka ingestion timestamp to timestamp.

    Parameters:
    - bronze_df: DataFrame -> Raw events coming from the Bronze layer.
    Returns:
    - DataFrame: Cleaned and normalized events.
    """


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

    return cleaned_df


def get_valid_condition() -> object:
    """
    Data Quality rules
    
    An event is valid if :
        - event_id exists
        - user_id > 0
        - product_id > 0
        - event_type is one of view, add_to_cart or purchase
        - timestamp exists
        - view/add_to_cart have no price(can be NULL)
        - purchase events have a positive price
    Returns:
    - Column -> Spark boolean expression representing the validation rules.
    """

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
            # view and add_to_cart: price must be NULL
            (
                F.col("event_type").isin(
                    "view",
                    "add_to_cart"
                )
                & F.col("price").isNull()
            )

            | #or

            # purchase: price required and strictly positive
            (
                (F.col("event_type") == "purchase")
                & F.col("price").isNotNull()
                & (F.col("price") > 0)
            )
        )
    )

    return valid_condition


def build_valid_events(cleaned_df: DataFrame) -> DataFrame:
    """
    VALID EVENTS
    Build the Silver dataset from cleaned events.
    then apply:
        watermark based on event time "timestamp".
        deduplication = event_id
    This allows Spark to handle late-arriving events while enabling the gradual cleanup of the state used for deduplication.
    Parameters:
    - cleaned_df : DataFrame -> Cleaned and normalized Bronze events.
    Returns:
    - DataFrame -> Valid, watermarked and deduplicated events.
    """


    valid_condition = get_valid_condition()

    silver_df = (
        cleaned_df
        .filter(valid_condition)
        .withWatermark(
            "timestamp",
            WATERMARK_DELAY
        )
        .dropDuplicates(
            ["event_id"]
        )
    )

    return silver_df


def build_quarantine_events(cleaned_df: DataFrame) -> DataFrame:
    """
    Build the quarantine dataset from invalid events.
    INVALID EVENTS / QUARANTINE
    Invalid events are NOT deleted. A "rejection_reason" column is added to indicate why the event was rejected.
    Parameters:
    - cleaned_df : DataFrame -> Cleaned and normalized Bronze events.
    Returns:
    - DataFrame -> Invalid events with a rejection_reason column.
    """

    valid_condition = get_valid_condition()

    quarantine_df = (
        cleaned_df
        .filter(~valid_condition)
         # Identify the reason for rejection.
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

    return quarantine_df


# Main streaming application

def main():
    """
    Start the Silver streaming pipeline.

    The main function is responsible for:

        - creating the Spark session,
        - reading the Bronze streaming source,
        - applying transformations,
        - writing Silver,
        - writing Quarantine.
    """

    # Spark Session

    spark = create_spark_session("ecommerce-silver")


    # Read Bronze

    bronze_df = (
        spark.readStream
        .format("parquet")
        .schema(BRONZE_SCHEMA)
        .load(BRONZE_PATH)
    )


    # Clean and normalize Bronze events.

    cleaned_df = clean_events(bronze_df)


    # Build valid Silver events.

    silver_df = build_valid_events(cleaned_df)


    # Build invalid events for Quarantine.

    quarantine_df = build_quarantine_events(cleaned_df)


    # Silver output
    # append mode means each micro-batch adds newly processed valid events to the Silver Parquet dataset.

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
            SILVER_CHECKPOINT_PATH
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
            QUARANTINE_CHECKPOINT_PATH
        )
        .start()
    )


    # Keep both streaming queries alive

    spark.streams.awaitAnyTermination()


# Application entry point

if __name__ == "__main__":
    main()