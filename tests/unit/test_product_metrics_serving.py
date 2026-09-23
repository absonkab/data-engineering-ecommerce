from datetime import datetime
from decimal import Decimal

from pyspark.sql.types import (
    DecimalType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.serving.publish_product_metrics import (
    PRODUCT_METRICS_COLUMNS,
    prepare_product_metrics,
)


def test_prepare_product_metrics_selects_serving_contract(spark):
    """
    Verify that the Product Metrics publisher exposes exactly
    the columns defined by the Serving contract.
    """

    # Define the schema explicitly to keep the test aligned
    # with the PostgreSQL Serving contract.
    schema = StructType(
        [
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("product_id", LongType(), False),
            StructField("total_events", LongType(), False),
            StructField("views", LongType(), False),
            StructField("add_to_carts", LongType(), False),
            StructField("purchases", LongType(), False),
            StructField("revenue", DecimalType(18, 2), False),
            StructField("unique_users", LongType(), False),
            StructField("unique_viewers", LongType(), False),
            StructField("unique_buyers", LongType(), False),
            StructField("conversion_rate", DecimalType(10, 4), False),
            StructField("internal_column", LongType(), False),
        ]
    )

    df = spark.createDataFrame(
        [
            (
                datetime(2026, 9, 23, 10, 0, 0),
                datetime(2026, 9, 23, 11, 0, 0),
                101,
                100,
                60,
                30,
                10,
                Decimal("500.00"),
                40,
                35,
                10,
                Decimal("0.2857"),
                999,
            )
        ],
        schema=schema,
    )

    prepared_df = prepare_product_metrics(df)

    # Only the Serving contract must be exposed.
    assert prepared_df.columns == PRODUCT_METRICS_COLUMNS

    # Additional Gold columns must not reach PostgreSQL.
    assert "internal_column" not in prepared_df.columns