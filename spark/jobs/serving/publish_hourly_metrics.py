from pyspark.sql import DataFrame, SparkSession
from jobs.serving.common.postgres import (
    cleanup_staging,
    upsert_from_staging,
    write_to_staging,
)
from jobs.serving.common.spark import get_spark_session

# Configuration 

from config.config import (
    GOLD_PATH,
    GOLD_HOURLY_METRICS_TABLE
)
HOURLY_GOLD_PATH = f"{GOLD_PATH}/hourly_metrics"
STAGING_TABLE = f"{GOLD_HOURLY_METRICS_TABLE}_staging"

# Columns shared by Gold and Serving.
HOURLY_METRICS_COLUMNS = [
    "window_start",
    "window_end",
    "total_events",
    "views",
    "add_to_carts",
    "purchases",
    "revenue",
    "unique_users",
]

# Business key used to identify a unique hourly window.
HOURLY_METRICS_CONFLICT_COLUMNS = [
    "window_start",
    "window_end",
]


# Reading the Gold dataset

def read_hourly_metrics(spark: SparkSession) -> DataFrame:
    """
    Reads Gold hourly metrics from Parquet storage.
    """
    return spark.read.parquet(HOURLY_GOLD_PATH)


# Preparing data for PostgreSQL

def prepare_hourly_metrics(df: DataFrame) -> DataFrame:
    """
    Prepares the Gold DataFrame before writing it to PostgreSQL. 
    Only the columns corresponding to the gold_hourly_metrics serving table are retained.
    """
    return df.select(*HOURLY_METRICS_COLUMNS)


# Main publication workflow

def main() -> None:
    """
    Publish Gold hourly metrics into PostgreSQL Serving.
    """

    # Create the SparkSession through the shared infrastructure
    spark = get_spark_session(
        "ServingHourlyMetricsPublisher"
    )

    try:
        # Gold Reading
        gold_df = read_hourly_metrics(spark)

        # Prepare the Serving dataset
        prepared_df = prepare_hourly_metrics(gold_df)

        # Write the current Gold snapshot to staging
        write_to_staging(df=prepared_df, staging_table=STAGING_TABLE,)

        # UPSERT in PostgreSQL: Merge staging into the final Serving table
        upsert_from_staging(
            spark=spark,
            staging_table=STAGING_TABLE,
            target_table=GOLD_HOURLY_METRICS_TABLE,
            columns=HOURLY_METRICS_COLUMNS,
            conflict_columns=HOURLY_METRICS_CONFLICT_COLUMNS,
        )


        # Cleanup: Remove temporary staging table
        cleanup_staging(
            spark=spark,
            staging_table=STAGING_TABLE,
        )

        print("Hourly metrics successfully published to PostgreSQL.")

    finally:
        spark.stop()


# Application Entrypoint

if __name__ == "__main__":
    main()