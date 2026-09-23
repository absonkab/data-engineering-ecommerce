from datetime import datetime
from decimal import Decimal

from pyspark.sql.types import (
    DecimalType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.serving.publish_hourly_metrics import (
    HOURLY_METRICS_COLUMNS,
    prepare_hourly_metrics,
)


def test_prepare_hourly_metrics_selects_serving_contract(spark):
    """
    Verify that the Hourly Metrics publisher exposes exactly
    the columns defined by the Serving contract.
    """


    # Define the schema explicitly to keep the test aligned
    # with the PostgreSQL Serving contract.
    schema = StructType(
        [
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("total_events", LongType(), False),
            StructField("views", LongType(), False),
            StructField("add_to_carts", LongType(), False),
            StructField("purchases", LongType(), False),
            StructField("revenue", DecimalType(18, 2), False),
            StructField("unique_users", LongType(), False),
            StructField("internal_column", LongType(), False),
        ]
    )

    df = spark.createDataFrame(
        [
            (
                datetime(2026, 9, 23, 10, 0, 0),
                datetime(2026, 9, 23, 11, 0, 0),
                100,
                60,
                30,
                10,
                Decimal("500.00"),
                40,
                999,
            )
        ],
        schema=schema,
    )

    prepared_df = prepare_hourly_metrics(df)

    # Only the Serving contract must be exposed.
    assert prepared_df.columns == HOURLY_METRICS_COLUMNS

    # Additional Gold columns must not reach PostgreSQL.
    assert "internal_column" not in prepared_df.columns