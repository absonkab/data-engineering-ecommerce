from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


# Configuration 

from config.config import (
    POSTGRES_PROPERTIES,
    GOLD_PATH,
    GOLD_HOURLY_METRICS_TABLE
)
HOURLY_GOLD_PATH = f"{GOLD_PATH}/hourly_metrics"
STAGING_TABLE = f"{GOLD_HOURLY_METRICS_TABLE}_staging"


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

    return df.select(
        "window_start",
        "window_end",
        "total_events",
        "views",
        "add_to_carts",
        "purchases",
        "revenue",
        "unique_users",
    )


# Writing to the staging table

def write_to_staging(df: DataFrame) -> None:
    """
    Writes the data to a temporary staging table. 
    This step then enables PostgreSQL to perform the UPSERT into the final table.
    """

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_PROPERTIES["url"])
        .option("dbtable", STAGING_TABLE)
        .option("user", POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver", POSTGRES_PROPERTIES["driver"])
        .mode("overwrite")
        .save()
    )


# PostgreSQL UPSERT

def upsert_to_postgres(spark: SparkSession) -> None:
    """
    Merges staging data into the final serving table. 
    The primary key (window_start, window_end) makes the operation idempotent:
        - new window -> INSERT
        - existing window -> UPDATE
    """

    # The PostgreSQL driver is already present in the Spark image.
    # JDBC is used to execute SQL operations.
    connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
        POSTGRES_PROPERTIES["url"],
        POSTGRES_PROPERTIES["user"],
        POSTGRES_PROPERTIES["password"],
    )

    try:
        statement = connection.createStatement()

        sql = f"""
        INSERT INTO {GOLD_HOURLY_METRICS_TABLE} (
            window_start,
            window_end,
            total_events,
            views,
            add_to_carts,
            purchases,
            revenue,
            unique_users
        )
        SELECT
            window_start,
            window_end,
            total_events,
            views,
            add_to_carts,
            purchases,
            revenue,
            unique_users
        FROM {STAGING_TABLE}
        ON CONFLICT (window_start, window_end)
        DO UPDATE SET
            total_events = EXCLUDED.total_events,
            views = EXCLUDED.views,
            add_to_carts = EXCLUDED.add_to_carts,
            purchases = EXCLUDED.purchases,
            revenue = EXCLUDED.revenue,
            unique_users = EXCLUDED.unique_users;
        """

        statement.executeUpdate(sql)

        statement.close()

    finally:
        connection.close()


# Cleanup staging

def cleanup_staging(spark: SparkSession) -> None:
    """
    Remove the temporary staging table after a successful
    publication.
    """

    connection = (
        spark._sc._gateway.jvm.java.sql.DriverManager
        .getConnection(
            POSTGRES_PROPERTIES["url"],
            POSTGRES_PROPERTIES["user"],
            POSTGRES_PROPERTIES["password"],
        )
    )

    try:
        statement = connection.createStatement()

        # The staging table is used only during this publication.
        statement.executeUpdate(
            f"DROP TABLE IF EXISTS {STAGING_TABLE};"
        )

        statement.close()

    finally:
        connection.close()



# Main

def main() -> None:
    """
    Publish Gold hourly metrics into PostgreSQL Serving.
    """

    spark = (
        SparkSession.builder
        .appName("ServingHourlyMetricsPublisher")
        .getOrCreate()
    )

    try:
        # Gold Reading
        gold_df = read_hourly_metrics(spark)

        # Data preparation
        prepared_df = prepare_hourly_metrics(gold_df)

        # Publication in the staging table
        write_to_staging(prepared_df)

        # UPSERT in PostgreSQL
        upsert_to_postgres(spark)

        # Cleanup
        cleanup_staging(spark)

        print("Hourly metrics successfully published to PostgreSQL.")

    finally:
        spark.stop()


# Application Entrypoint

if __name__ == "__main__":
    main()