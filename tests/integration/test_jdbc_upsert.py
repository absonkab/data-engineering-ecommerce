from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    LongType,
    DecimalType,
)
from datetime import datetime
from decimal import Decimal
import time
from jobs.serving.publish_hourly_metrics import prepare_hourly_metrics, write_to_staging, upsert_to_postgres, cleanup_staging


# PostgreSQL Configuration 

from config.config import (
    POSTGRES_URL,
    POSTGRES_PROPERTIES,
    GOLD_PATH,
    GOLD_HOURLY_METRICS_TABLE
)
HOURLY_GOLD_PATH = f"{GOLD_PATH}/hourly_metrics"
STAGING_TABLE = f"{GOLD_HOURLY_METRICS_TABLE}_staging"


# Spark session creation

spark = (
    SparkSession.builder
    .appName("JDBCUpsertTest")
    .master("local[1]")
    .getOrCreate()
)


try:

    # Create a test DataFrame
    # We simulate a first publication of the window 10:00 -> 11:00.
    # and a second one with the same window

    schema = StructType([
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("total_events", LongType(), False),
        StructField("views", LongType(), False),
        StructField("add_to_carts", LongType(), False),
        StructField("purchases", LongType(), False),
        StructField("revenue", DecimalType(18, 2), False),
        StructField("unique_users", LongType(), False),
    ])

    # test data
    data = [
        (
            datetime(2000, 9, 1, 10, 0, 0),
            datetime(2000, 9, 1, 11, 0, 0),
            100,
            60,
            30,
            10,
            Decimal("500.00"),
            40,
        ),
        
        (
            datetime(2000, 9, 1, 10, 0, 0),
            datetime(2000, 9, 1, 11, 0, 0),
            105,
            63,
            32,
            11,
            Decimal("550.00"),
            42,
        )
    ]

    df = spark.createDataFrame(data, schema)

    # Data preparation
    prepared_df = prepare_hourly_metrics(df)

    print("\n=== TEST DATA ===")
    prepared_df.show(truncate=False)


    # Writting in the staging table

    print("\n=== WRITING TO STAGING ===")

    write_to_staging(prepared_df)

    print("STAGING_WRITE_OK")


    # Native JDBC connection to PostgreSQL

    connection = (
        spark._sc._gateway.jvm.java.sql.DriverManager
        .getConnection(
            POSTGRES_URL,
            POSTGRES_PROPERTIES["user"],
            POSTGRES_PROPERTIES["password"],
        )
    )

    try:

        statement = connection.createStatement()

        rows = df.collect()

        # UPSERT each row toward finale table
        # We can't so use upsert_to_postgres() here

        for row in rows:
            upsert_sql = f"""
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
                VALUES (
                    '{row.window_start}',
                    '{row.window_end}',
                    {row.total_events},
                    {row.views},
                    {row.add_to_carts},
                    {row.purchases},
                    {row.revenue},
                    {row.unique_users}
                )
                ON CONFLICT (window_start, window_end)
                DO UPDATE SET
                    total_events = EXCLUDED.total_events,
                    views = EXCLUDED.views,
                    add_to_carts = EXCLUDED.add_to_carts,
                    purchases = EXCLUDED.purchases,
                    revenue = EXCLUDED.revenue,
                    unique_users = EXCLUDED.unique_users;
            """

            statement.executeUpdate(upsert_sql)

            print("UPSERT_OK")

        # Checking that we have only the second row

        result = statement.executeQuery(f"""
            SELECT
                window_start,
                window_end,
                total_events,
                views,
                purchases,
                revenue
            FROM {GOLD_HOURLY_METRICS_TABLE}
            WHERE window_start = '2000-09-01 10:00:00'
              AND window_end = '2000-09-01 11:00:00';
        """)

        print("\n=== POSTGRES RESULT ===")

        while result.next():
            print("+---------------------------------------------------------------------------------------------------------------------------------------------+")
            print(
                "| window_start =", result.getString("window_start"),
                "| window_end =", result.getString("window_end"),
                "| total_events =", result.getLong("total_events"),
                "| views =", result.getLong("views"),
                "| purchases =", result.getLong("purchases"),
                "| revenue =", result.getBigDecimal("revenue"),
                "|",
            )
            print("+---------------------------------------------------------------------------------------------------------------------------------------------+")


        # Staging cleanup

        cleanup_staging(spark)

        print("\nSTAGING_CLEANUP_OK")

        # Delete the test row
        print("\nTest row will be deleted in 30s\nElse don't forget to delete it manually: DELETE FROM gold_hourly_metrics WHERE window_start = '2000-09-01 10:00:00' AND window_end = '2000-09-01 11:00:00';")
        delete_test_row_sql = f"""DELETE FROM {GOLD_HOURLY_METRICS_TABLE} WHERE window_start = '2000-09-01 10:00:00' AND window_end = '2000-09-01 11:00:00';"""

        # Wait 30 seconds
        time.sleep(30)
        rows_deleted = statement.executeUpdate(delete_test_row_sql)
        print(f"{rows_deleted} deleted line. Test row is successfully deleted")

        statement.close()

    finally:
        connection.close()


finally:
    spark.stop()