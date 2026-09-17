"""
Unit tests for the Silver transformation layer.

These tests validate:
    - data cleaning and normalization,
    - valid event detection,
    - quarantine of invalid events,
    - rejection reasons.
"""

from jobs.silver_stream import (
    build_quarantine_events,
    build_valid_events,
    clean_events,
)


def create_test_data(spark):
    """
    Create a small DataFrame containing representative Bronze events.

    The dataset contains:
        - one valid view,
        - one valid purchase,
        - one event with a missing event_id,
        - one event with an invalid user_id,
        - one event with an invalid event_type,
        - one purchase with an invalid price,
        - one event requiring normalization.
    The DataFrame also contains the Kafka metadata expected by the Bronze/Silver contract.
    """

    data = [
        # Valid view
        (
            "event-001",
            101,
            201,
            "view",
            None,
            "2026-09-16 10:00:00",
            "ecommerce_events",
            0,
            1001,
            "2026-09-16 10:00:01",
        ),

        # Valid purchase
        (
            "event-002",
            102,
            202,
            "purchase",
            49.99,
            "2026-09-16 10:05:00",
            "ecommerce_events",
            0,
            1002,
            "2026-09-16 10:05:01",
        ),

        # Missing event_id
        (
            None,
            103,
            203,
            "view",
            None,
            "2026-09-16 10:10:00",
            "ecommerce_events",
            0,
            1003,
            "2026-09-16 10:10:01",
        ),

        # Invalid user_id
        (
            "event-004",
            -1,
            204,
            "view",
            None,
            "2026-09-16 10:15:00",
            "ecommerce_events",
            0,
            1004,
            "2026-09-16 10:15:01",
        ),

        # Invalid event_type
        (
            "event-005",
            105,
            205,
            "unknown",
            None,
            "2026-09-16 10:20:00",
            "ecommerce_events",
            0,
            1005,
            "2026-09-16 10:20:01",
        ),

        # Invalid purchase price
        (
            "event-006",
            106,
            206,
            "purchase",
            0.0,
            "2026-09-16 10:25:00",
            "ecommerce_events",
            0,
            1006,
            "2026-09-16 10:25:01",
        ),

        # Event requiring normalization
        (
            "  event-007  ",
            107,
            207,
            "  VIEW  ",
            None,
            "2026-09-16 10:30:00",
            "ecommerce_events",
            0,
            1007,
            "2026-09-16 10:30:01",
        ),
    ]

    columns = [
        "event_id",
        "user_id",
        "product_id",
        "event_type",
        "price",
        "timestamp",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    ]

    return spark.createDataFrame(data, columns)


def test_clean_events_normalizes_values(spark):
    """
    Verify that event_id and event_type are normalized correctly.
    """

    df = create_test_data(spark)

    cleaned_df = clean_events(df)

    event = (
        cleaned_df
        .filter(cleaned_df.event_id == "event-007")
        .select("event_id", "event_type")
        .first()
    )

    assert event["event_id"] == "event-007"
    assert event["event_type"] == "view"


def test_build_valid_events_keeps_valid_events(spark):
    """
    Verify that valid events are retained by the Silver layer.
    """

    df = create_test_data(spark)

    cleaned_df = clean_events(df)
    silver_df = build_valid_events(cleaned_df)

    event_ids = {
        row["event_id"]
        for row in silver_df.select("event_id").collect()
    }

    # valid event must be in silver
    assert "event-001" in event_ids
    assert "event-002" in event_ids
    assert "event-007" in event_ids


def test_build_valid_events_rejects_invalid_events(spark):
    """
    Verify that invalid events do not reach the Silver layer.
    """

    df = create_test_data(spark)

    cleaned_df = clean_events(df)
    silver_df = build_valid_events(cleaned_df)

    event_ids = {
        row["event_id"]
        for row in silver_df.select("event_id").collect()
    }

    # invalid event must not be in silver
    assert "event-004" not in event_ids
    assert "event-005" not in event_ids
    assert "event-006" not in event_ids


def test_quarantine_contains_invalid_events(spark):
    """
    Verify that invalid events are redirected to quarantine.
    """

    df = create_test_data(spark)

    cleaned_df = clean_events(df)
    quarantine_df = build_quarantine_events(cleaned_df)

    event_ids = {
        row["event_id"]
        for row in quarantine_df.select("event_id").collect()
    }

    # invalid event must be in quarantine
    assert "event-004" in event_ids
    assert "event-005" in event_ids
    assert "event-006" in event_ids

    # valid event must not be in quarantine
    assert "event-001" not in event_ids
    assert "event-002" not in event_ids
    assert "event-007" not in event_ids


def test_quarantine_rejection_reasons(spark):
    """
    Verify that invalid events receive the expected rejection reason.
    """

    df = create_test_data(spark)

    cleaned_df = clean_events(df)
    quarantine_df = build_quarantine_events(cleaned_df)

    reasons = {
        row["event_id"]: row["rejection_reason"]
        for row in quarantine_df
        .select("event_id", "rejection_reason")
        .collect()
    }

    assert reasons["event-004"] == "invalid_user_id"
    assert reasons["event-005"] == "invalid_event_type"
    assert reasons["event-006"] == "invalid_purchase_price"


def test_quarantine_rejection_reason_for_missing_event_id(spark):
    """
    Verify that an event with a missing event_id
    receives the correct rejection reason.
    """

    df = create_test_data(spark)

    cleaned_df = clean_events(df)
    quarantine_df = build_quarantine_events(cleaned_df)

    event = (
        quarantine_df
        .filter(quarantine_df.rejection_reason == "missing_event_id")
        .select("event_id", "rejection_reason")
        .first()
    )

    assert event is not None
    assert event["event_id"] is None
    assert event["rejection_reason"] == "missing_event_id"