from datetime import datetime
from decimal import Decimal

from pyspark.sql.types import (
    LongType,
    DecimalType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.serving.publish_user_metrics import (
    USER_METRICS_COLUMNS,
    prepare_user_metrics,
)


def test_prepare_user_metrics_selects_serving_contract(spark):
    """
    Verify that the User Metrics publisher exposes exactly
    the columns defined by the Serving contract.
    """

    # Define the schema explicitly so that the test uses
    # the same data types expected by the Serving layer.
    schema = StructType(
        [
            StructField("window_start", TimestampType(), nullable=False),
            StructField("window_end", TimestampType(), nullable=False),
            StructField("user_id", LongType(), nullable=False),
            StructField("total_events", LongType(), nullable=False),
            StructField("views", LongType(), nullable=False),
            StructField("add_to_carts", LongType(), nullable=False),
            StructField("purchases", LongType(), nullable=False),
            StructField("revenue", DecimalType(18, 2), nullable=False),
            StructField("unique_products", LongType(), nullable=False),
            StructField("purchase_rate", DecimalType(10, 4), nullable=False),
            StructField("internal_column", LongType(), nullable=False),
        ]
    )

    # Create a small DataFrame containing the Serving columns
    # plus an additional column that must not be published.
    df = spark.createDataFrame(
        [
            (
                datetime(2026, 9, 23, 10, 0, 0),
                datetime(2026, 9, 23, 11, 0, 0),
                42,
                10,
                5,
                3,
                2,
                Decimal("100.50"),
                4,
                Decimal("0.2000"),
                999,
            )
        ],
        schema=schema,
    )

    # Apply the publisher's Serving preparation logic.
    prepared_df = prepare_user_metrics(df)

    # The publisher must expose exactly the Serving contract.
    assert prepared_df.columns == USER_METRICS_COLUMNS

    # Additional Gold columns must not leak into PostgreSQL.
    assert "internal_column" not in prepared_df.columns