"""
Tests for the user-level Gold metrics transformation.

These tests validate the aggregations produced by
user_metrics.py without requiring Kafka or a streaming query.
"""

from datetime import datetime

from jobs.user_metrics import build_user_metrics


def create_test_data(spark):
    """
    Create Silver-like events for user metrics testing.

    User 101:
        - 2 views
        - 1 add_to_cart
        - 1 purchase
        - 2 unique products
        - 4 total events

    User 102:
        - 1 view
        - 1 add_to_cart
        - 1 purchase
        - 1 unique product
        - 3 total event
    
    User 103:
        - 2 view
        - 1 add_to_cart
        - 2 unique products
        - 3 total events
    """

    data = [
        # User 101 - Product 201
        (101, 201, "view", None, datetime(2026, 9, 8, 10, 10, 0)),
        (101, 201, "add_to_cart", None, datetime(2026, 9, 8, 10, 20, 0)),
        (101, 201, "purchase", 49.99, datetime(2026, 9, 8, 10, 30, 0)),

        # User 101 - Product 202
        (101, 202, "view", None, datetime(2026, 9, 8, 10, 40, 0)),

        # User 102 - Product 203
        (102, 203, "view", None, datetime(2026, 9, 8, 10, 47, 0)),
        (102, 203, "add_to_cart", None, datetime(2026, 9, 8, 10, 54, 0)),
        (102, 203, "purchase", 29.99, datetime(2026, 9, 8, 10, 59, 0)),

        # User 103 - Product 204
        (103, 204, "view", None, datetime(2026, 9, 8, 10, 15, 0)),

        # User 103 - Product 205
        (103, 205, "view", None, datetime(2026, 9, 8, 10, 20, 0)),
        (103, 205, "add_to_cart", None, datetime(2026, 9, 8, 10, 25, 0)),
    ]

    columns = [
        "user_id",
        "product_id",
        "event_type",
        "price",
        "timestamp",
    ]

    return spark.createDataFrame(data, columns)


def test_build_user_metrics_creates_one_row_per_user(spark):
    """
    Verify that each user gets its own aggregated row.
    """

    df = create_test_data(spark)

    result = build_user_metrics(df)

    rows = result.orderBy("user_id").collect()

    assert len(rows) == 3

    assert rows[0]["user_id"] == 101
    assert rows[1]["user_id"] == 102
    assert rows[2]["user_id"] == 103


def test_build_user_metrics_counts_event_types(spark):
    """
    Verify event-type aggregations for user 101.
    """

    df = create_test_data(spark)

    result = build_user_metrics(df)

    user_101 = (
        result
        .filter(result.user_id == 101)
        .first()
    )

    assert user_101 is not None

    assert user_101["total_events"] == 4
    assert user_101["views"] == 2
    assert user_101["add_to_carts"] == 1
    assert user_101["purchases"] == 1


def test_build_user_metrics_calculates_revenue(spark):
    """
    Verify that revenue is correctly calculated for user 101.
    """

    df = create_test_data(spark)

    result = build_user_metrics(df)

    user_101 = (
        result
        .filter(result.user_id == 101)
        .first()
    )

    assert user_101 is not None

    assert abs(user_101["revenue"] - 49.99) < 0.001


def test_build_user_metrics_counts_unique_products(spark):
    """
    Verify the number of unique products interacted with by user 103.
    """

    df = create_test_data(spark)

    result = build_user_metrics(df)

    user_103 = (
        result
        .filter(result.user_id == 103)
        .first()
    )

    assert user_103 is not None

    # User 103 interacted with products 204 and 205.
    assert user_103["unique_products"] == 2


def test_build_user_metrics_calculates_purchase_rate(spark):
    """
    Verify the purchase rate.

    For user 102:
        1 purchase / 3 total events = 0.333
    """

    df = create_test_data(spark)

    result = build_user_metrics(df)

    user_102 = (
        result
        .filter(result.user_id == 102)
        .first()
    )

    assert user_102 is not None

    assert abs(user_102["purchase_rate"] - 0.333) < 0.001


def test_build_user_metrics_handles_user_without_purchase(spark):
    """
    Verify purchase rate when a user has no purchase (user 103).

    This also ensures that the aggregation returns zero
    instead of producing an invalid value.
    """

    df = create_test_data(spark)

    result = build_user_metrics(df)

    user_103 = (
        result
        .filter(result.user_id == 103)
        .first()
    )

    assert user_103 is not None

    assert user_103["purchases"] == 0
    assert user_103["purchase_rate"] == 0.0