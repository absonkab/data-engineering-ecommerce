"""
Gold streaming job.

This job reads valid events from the Silver layer
and produces hourly aggregated business metrics.

Metrics produced:
- total_events
- views
- add_to_carts
- purchases
- revenue
- unique_users

The timestamp column represents the event time.
A 10-minute watermark handles late-arriving events
while limiting the state maintained by Spark.
"""

from pyspark.sql import functions as F
from utils.spark_session import create_spark_session
from schemas.ecommerce_schema import BRONZE_SCHEMA


# Spark session

spark = create_spark_session("ecommerce-gold")


# Paths

SILVER_PATH = "/lake/silver"
GOLD_PATH = "/lake/gold/hourly_metrics"
GOLD_CHECKPOINT = "/lake/checkpoints/gold/hourly_metrics"


# Read Silver

silver_df = (
    spark.readStream
    .format("parquet")
    .schema(BRONZE_SCHEMA)
    .load(SILVER_PATH)
)


# Prepare event time

prepared_df = (
    silver_df
    .withColumn(
        "timestamp",
        F.col("timestamp").cast("timestamp")
    )
)


# Watermark

watermarked_df = (
    prepared_df
    .withWatermark("timestamp", "10 minutes")
)


# Hourly aggregations

hourly_metrics_df = (
    watermarked_df
    .groupBy(
        F.window(
            F.col("timestamp"),
            "1 hour"
        )
    )
    .agg(
        F.count("*").alias("total_events"),

        F.sum(
            F.when(
                F.col("event_type") == "view",
                1
            ).otherwise(0)
        ).alias("views"),

        F.sum(
            F.when(
                F.col("event_type") == "add_to_cart",
                1
            ).otherwise(0)
        ).alias("add_to_carts"),

        F.sum(
            F.when(
                F.col("event_type") == "purchase",
                1
            ).otherwise(0)
        ).alias("purchases"),

        F.sum(
            F.when(
                F.col("event_type") == "purchase",
                F.col("price")
            ).otherwise(0)
        ).alias("revenue"),

        F.approx_count_distinct("user_id").alias("unique_users")
    )
)


# Flatten the window structure

gold_df = (
    hourly_metrics_df
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "total_events",
        "views",
        "add_to_carts",
        "purchases",
        "revenue",
        "unique_users"
    )
)


# Write Gold

gold_query = (
    gold_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", GOLD_PATH)
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .start()
)


# Keep the streaming query alive

gold_query.awaitTermination()