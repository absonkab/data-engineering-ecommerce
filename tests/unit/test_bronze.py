"""
Tests for the Bronze transformation layer.

These tests validate the transformation of raw Kafka records
into structured Bronze records.

The tests do not require a real Kafka broker.
We directly create a Spark DataFrame that reproduces
the structure returned by the Kafka source.
"""

from jobs.bronze_stream import parse_kafka_events


def create_kafka_test_data(spark):
    """
    Create a test DataFrame that mimics Spark's Kafka source.

    Kafka normally provides:
        - value
        - topic
        - partition
        - offset
        - timestamp

    The value contains the raw JSON event.
    """

    data = [
        (
            '{"event_id":"event-001",'
            '"user_id":101,'
            '"product_id":201,'
            '"event_type":"view",'
            '"price":null,'
            '"timestamp":"2026-09-08T10:00:00"}',
            "ecommerce_events",
            0,
            100,
            "2026-09-08 10:00:01",
        ),
        (
            '{"event_id":"event-002",'
            '"user_id":102,'
            '"product_id":202,'
            '"event_type":"purchase",'
            '"price":49.99,'
            '"timestamp":"2026-09-08T10:01:00"}',
            "ecommerce_events",
            1,
            101,
            "2026-09-08 10:01:01",
        ),

        # malformed/incomplete event from kafka
        (
            '{"event_id":null,'
            '"user_id":-1,'
            '"product_id":201,'
            '"event_type":"unknown",'
            '"price":null,'
            '"timestamp":"2026-09-08T10:00:00"}',
            "ecommerce_events",
            0,
            102,
            "2026-09-08 10:02:00",
        )
    ]

    columns = [
        "value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    ]

    return spark.createDataFrame(data, columns)


def test_parse_kafka_events_parses_json(spark):
    """
    Verify that the raw Kafka JSON payload is correctly parsed
    into the expected business columns.
    """

    kafka_df = create_kafka_test_data(spark)

    bronze_df = parse_kafka_events(kafka_df)

    # order to be sure that the first one is none id event
    events = bronze_df.orderBy("event_id").collect()

    assert len(events) == 3

    assert events[1]["event_id"] == "event-001"
    assert events[1]["user_id"] == 101
    assert events[1]["product_id"] == 201
    assert events[1]["event_type"] == "view"

    assert events[2]["event_id"] == "event-002"
    assert events[2]["user_id"] == 102
    assert events[2]["product_id"] == 202
    assert events[2]["event_type"] == "purchase"
    assert events[2]["price"] == 49.99


def test_parse_kafka_events_preserves_kafka_metadata(spark):
    """
    Verify that Kafka metadata is preserved for traceability.
    """

    kafka_df = create_kafka_test_data(spark)

    bronze_df = parse_kafka_events(kafka_df)

    event = (
        bronze_df
        .filter(bronze_df.event_id == "event-001")
        .first()
    )

    assert event is not None
    assert event["kafka_topic"] == "ecommerce_events"
    assert event["kafka_partition"] == 0
    assert event["kafka_offset"] == 100


def test_parse_kafka_events_keeps_invalid_fields_for_silver(spark):
    """
    Verify that malformed or incomplete business data is not
    automatically discarded by the Bronze layer.
    """

    kafka_df = create_kafka_test_data(spark)

    bronze_df = parse_kafka_events(kafka_df)

    # collect the third event which contains malformed data
    event = bronze_df.collect()[2]


    assert event is not None

    # Bronze keeps the invalid values so that Silver
    # can identify and quarantine the event.
    assert event["event_id"] is None
    assert event["user_id"] == -1
    assert event["event_type"] == "unknown"