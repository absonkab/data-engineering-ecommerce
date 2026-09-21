"""
Tests for the product-level Gold metrics transformation.

These tests validate the aggregations produced by
product_metrics.py without requiring Kafka or a streaming query.
"""

from datetime import datetime

from jobs.product_metrics import build_product_metrics


def create_test_data(spark):
    """
    Create Silver-like events for product metrics testing.

    Product 201:
        - 3 views
        - 2 add_to_cart
        - 2 purchase
        - 3 unique users

    Product 202:
        - 2 views
        - 1 add_to_cart
        - 0 purchase
        - 2 unique users
    """

    data = [
        # user 101 bought Product 201
        (101, 201, "view", None, datetime(2026, 9, 8, 10, 10, 0)),
        (101, 201, "add_to_cart", None, datetime(2026, 9, 8, 10, 20, 0)),
        (101, 201, "purchase", 49.99, datetime(2026, 9, 8, 10, 30, 0)),

        # user 102 only viewed Product 201
        (102, 201, "view", None, datetime(2026, 9, 8, 10, 40, 0)),

        # user 103 bought Product 201
        (103, 201, "view", None, datetime(2026, 9, 8, 10, 40, 0)),
        (103, 201, "add_to_cart", None, datetime(2026, 9, 8, 10, 45, 0)),
        (103, 201, "purchase", 49.99, datetime(2026, 9, 8, 10, 50, 0)),

        # user 102 added Product 202
        (102, 202, "view", 17.99, datetime(2026, 9, 8, 10, 25, 0)),
        (102, 202, "add_to_cart", 17.99, datetime(2026, 9, 8, 10, 35, 0)),

        # user 103 only viewed Product 202
        (103, 202, "view", 17.99, datetime(2026, 9, 8, 10, 25, 0)),
    ]

    columns = [
        "user_id",
        "product_id",
        "event_type",
        "price",
        "timestamp",
    ]

    return spark.createDataFrame(data, columns)


def test_build_product_metrics_creates_one_row_per_product(spark):
    """
    Verify that each product gets its own aggregated row.
    """

    df = create_test_data(spark)

    result = build_product_metrics(df)

    rows = result.orderBy("product_id").collect()

    assert len(rows) == 2

    assert rows[0]["product_id"] == 201
    assert rows[1]["product_id"] == 202


def test_build_product_metrics_counts_event_types(spark):
    """
    Verify event-type aggregations for products.
    """

    df = create_test_data(spark)

    result = build_product_metrics(df)

    # Agg for product 201
    product_201 = (
        result
        .filter(result.product_id == 201)
        .first()
    )

    assert product_201 is not None

    assert product_201["total_events"] == 7
    assert product_201["views"] == 3
    assert product_201["add_to_carts"] == 2
    assert product_201["purchases"] == 2

    # Agg for product 202
    product_202 = (
        result
        .filter(result.product_id == 202)
        .first()
    )

    assert product_202 is not None
    
    assert product_202["total_events"] == 3
    assert product_202["views"] == 2
    assert product_202["add_to_carts"] == 1
    assert product_202["purchases"] == 0


def test_build_product_metrics_calculates_revenue(spark):
    """
    Verify that revenue is correctly calculated per product.
    """

    df = create_test_data(spark)

    result = build_product_metrics(df)

    # Product 201 revenue
    product_201 = (
        result
        .filter(result.product_id == 201)
        .first()
    )

    assert product_201 is not None

    assert abs(product_201["revenue"] - 99.98) < 0.001

    # Product 202 revenue
    product_202 = (
        result
        .filter(result.product_id == 202)
        .first()
    )

    assert product_202 is not None

    assert product_202["revenue"] == 0.0


def test_build_product_metrics_counts_unique_users(spark):
    """
    Verify unique user counts for a product.
    """

    df = create_test_data(spark)

    result = build_product_metrics(df)

    # Product 201
    product_201 = (
        result
        .filter(result.product_id == 201)
        .first()
    )

    assert product_201 is not None

    # Users 101, 102 and 103 interacted with product 201.
    assert product_201["unique_users"] == 3

    # Product 202
    product_202 = (
        result
        .filter(result.product_id == 202)
        .first()
    )

    assert product_202 is not None

    # Users 102 and 103 interacted with product 202.
    assert product_202["unique_users"] == 2


def test_build_product_metrics_counts_unique_viewers_and_buyers(spark):
    """
    Verify unique viewers and buyers per product.
    """

    df = create_test_data(spark)

    result = build_product_metrics(df)

    # product 201
    product_201 = (
        result
        .filter(result.product_id == 201)
        .first()
    )

    assert product_201 is not None

    assert product_201["unique_viewers"] == 3
    assert product_201["unique_buyers"] == 2

    # product 202
    product_202 = (
        result
        .filter(result.product_id == 202)
        .first()
    )

    assert product_202 is not None

    assert product_202["unique_viewers"] == 2
    assert product_202["unique_buyers"] == 0


def test_build_product_metrics_calculates_conversion_rate(spark):
    """
    Verify the product conversion rate.

    For product 201:
        2 unique buyer / 3 unique viewers = 0.66
    For product 202:
        0 unique buyer / 2 unique viewers = 0.66
    """

    df = create_test_data(spark)

    result = build_product_metrics(df)

    # For product 201
    product_201 = (
        result
        .filter(result.product_id == 201)
        .first()
    )

    assert product_201 is not None

    assert abs(product_201["conversion_rate"] - 0.666) < 0.001

     # For product 202
    product_202 = (
        result
        .filter(result.product_id == 202)
        .first()
    )

    assert product_202 is not None

    assert abs(product_202["conversion_rate"] - 0.0) < 0.001