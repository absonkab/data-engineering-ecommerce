from jobs.serving.common.postgres import build_upsert_sql


def test_build_upsert_sql_does_not_update_conflict_columns():
    """
    Verify that business key columns are excluded from the
    PostgreSQL DO UPDATE SET clause.
    """

    columns = [
        "window_start",
        "window_end",
        "other_id",
        "changeable_field",
    ]

    conflict_columns = [
        "window_start",
        "window_end",
        "other_id",
    ]

    sql = build_upsert_sql(
        staging_table="gold_metrics_staging",
        target_table="gold_metrics",
        columns=columns,
        conflict_columns=conflict_columns,
    )

    # Business key columns must not be updated.
    assert "window_start = EXCLUDED.window_start" not in sql
    assert "window_end = EXCLUDED.window_end" not in sql
    assert "other_id = EXCLUDED.other_id" not in sql

    # Business columns must be updated.
    assert "changeable_field = EXCLUDED.changeable_field" in sql


def test_build_upsert_sql_contains_expected_conflict_clause():
    """
    Verify that the generated SQL uses the expected business key
    in the PostgreSQL ON CONFLICT clause.
    """

    columns = [
        "window_start",
        "window_end",
        "other_id",
        "changeable_field1",
        "changeable_field2",
    ]

    conflict_columns = [
        "window_start",
        "window_end",
        "other_id",
    ]

    sql = build_upsert_sql(
        staging_table="gold_metrics_staging",
        target_table="gold_metrics",
        columns=columns,
        conflict_columns=conflict_columns,
    )

    # Verify the INSERT target table.
    assert "INSERT INTO gold_metrics" in sql

    # Verify the staging source.
    assert "FROM gold_metrics_staging" in sql

    # Verify the business key used by PostgreSQL.
    assert (
        "ON CONFLICT (window_start, window_end, other_id)"
        in sql
    )

    # Verify that all non-key columns are updated.
    assert "changeable_field1 = EXCLUDED.changeable_field1" in sql
    assert "changeable_field2 = EXCLUDED.changeable_field2" in sql