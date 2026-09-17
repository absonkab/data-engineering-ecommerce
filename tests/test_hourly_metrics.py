"""
Tests for the hourly Gold metrics transformation.

These tests validate the business aggregations produced by
the hourly_metrics job without requiring Kafka or a running
streaming query.
"""

from datetime import datetime

from jobs.hourly_metrics import build_hourly_metrics


def create_test_data(spark):
    """
    Create Silver-like events for testing the hourly aggregation.

    All events belong to the same one-hour window so that we can
    verify the resulting aggregation easily.
    """

    data = [
        # View
        (101, 201, "view", None, datetime(2026, 9, 8, 10, 10, 0)),

        # Add to cart
        (101, 201, "add_to_cart", None, datetime(2026, 9, 8, 10, 20, 0)),

        # Purchase
        (101, 201, "purchase", 49.99, datetime(2026, 9, 8, 10, 30, 0)),

        # Another purchase from another user
        (102, 202, "purchase", 29.99, datetime(2026, 9, 8, 10, 40, 0)),
    ]

    columns = [
        "user_id",
        "product_id",
        "event_type",
        "price",
        "timestamp",
    ]

    return spark.createDataFrame(data, columns)


def test_build_hourly_metrics_counts_events(spark):
    """
    Verify total events and event-type counts.
    """

    df = create_test_data(spark)

    result = build_hourly_metrics(df)

    row = result.first()

    assert row is not None

    assert row["total_events"] == 4
    assert row["views"] == 1
    assert row["add_to_carts"] == 1
    assert row["purchases"] == 2


def test_build_hourly_metrics_calculates_revenue(spark):
    """
    Verify that revenue is correctly calculated from purchases.
    """

    df = create_test_data(spark)

    result = build_hourly_metrics(df)

    row = result.first()

    assert row is not None

    # 49.99 + 29.99
    assert abs(row["revenue"] - 79.98) < 0.001


def test_build_hourly_metrics_counts_unique_users(spark):
    """
    Verify that the aggregation counts unique users.
    """

    df = create_test_data(spark)

    result = build_hourly_metrics(df)

    row = result.first()

    assert row is not None

    # Users 101 and 102
    assert row["unique_users"] == 2


def test_build_hourly_metrics_creates_one_hour_window(spark):
    """
    Verify that events are grouped into a one-hour event-time window.
    """

    df = create_test_data(spark)

    result = build_hourly_metrics(df)

    row = result.first()

    assert row is not None

    assert row["window_start"] == datetime(2026, 9, 8, 10, 0, 0)
    assert row["window_end"] == datetime(2026, 9, 8, 11, 0, 0)