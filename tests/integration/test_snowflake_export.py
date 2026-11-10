"""
Tests for the Gold to Snowflake export job.

These tests validate:
- Export dataset configuration.
- Source directory validation.
- Parquet file validation.
- Empty dataset handling.
- Parquet export generation.
- Export data integrity.

Tests use a local SparkSession and temporary directories.
No Kafka, Airflow or Snowflake connection is required.
"""

from pathlib import Path

import pytest

from jobs.export_gold_to_snowflake import (
    EXPORT_DATASETS,
    export_dataset,
)


# -------------------------------------------------------------------
# Test data
# -------------------------------------------------------------------

def create_test_data(spark):
    """
    Create a small dataset representing Gold business metrics.

    Three records are used to validate the export process.
    """

    data = [
        (201, 10, 3, 2, 99.98, 2),
        (202, 15, 5, 4, 149.95, 3),
        (203, 8, 2, 1, 29.99, 1),
    ]

    columns = [
        "product_id",
        "total_events",
        "views",
        "purchases",
        "revenue",
        "unique_users",
    ]

    return spark.createDataFrame(data, columns)


# -------------------------------------------------------------------
# Configuration tests
# -------------------------------------------------------------------

def test_export_datasets_contains_expected_datasets():
    """
    Verify that all expected Gold datasets are configured.
    """

    expected_datasets = {
        "hourly_metrics",
        "product_metrics",
        "user_metrics",
    }

    assert set(EXPORT_DATASETS.keys()) == expected_datasets


def test_export_datasets_have_source_and_destination():
    """
    Verify that every dataset has a source and destination path.
    """

    for dataset_name, paths in EXPORT_DATASETS.items():

        assert paths["source"]
        assert paths["destination"]

        assert paths["source"].endswith(dataset_name)
        assert paths["destination"].endswith(dataset_name)


# -------------------------------------------------------------------
# Source validation tests
# -------------------------------------------------------------------

def test_export_dataset_raises_error_when_source_does_not_exist(
    spark,
    tmp_path,
):
    """
    Verify that exporting a nonexistent source raises an error.
    """

    source_path = tmp_path / "nonexistent"
    destination_path = tmp_path / "export"

    with pytest.raises(FileNotFoundError):

        export_dataset(
            spark=spark,
            dataset_name="test_dataset",
            source_path=str(source_path),
            destination_path=str(destination_path),
        )


def test_export_dataset_raises_error_when_no_parquet_files_exist(
    spark,
    tmp_path,
):
    """
    Verify that a source directory without Parquet files is rejected.
    """

    source_path = tmp_path / "source"
    destination_path = tmp_path / "export"

    source_path.mkdir()

    # Create a non-Parquet file.
    (source_path / "readme.txt").write_text(
        "This directory contains no Parquet data."
    )

    with pytest.raises(FileNotFoundError):

        export_dataset(
            spark=spark,
            dataset_name="test_dataset",
            source_path=str(source_path),
            destination_path=str(destination_path),
        )


# -------------------------------------------------------------------
# Empty dataset tests
# -------------------------------------------------------------------

def test_export_dataset_raises_error_when_dataset_is_empty(
    spark,
    tmp_path,
):
    """
    Verify that an empty Parquet dataset cannot be exported.
    """

    source_path = tmp_path / "source"
    destination_path = tmp_path / "export"

    empty_df = spark.createDataFrame(
        [],
        "product_id INT, revenue DOUBLE",
    )

    # Write an empty Parquet dataset.
    empty_df.write.parquet(str(source_path))

    with pytest.raises(ValueError, match="is empty"):

        export_dataset(
            spark=spark,
            dataset_name="test_dataset",
            source_path=str(source_path),
            destination_path=str(destination_path),
        )


# -------------------------------------------------------------------
# Successful export tests
# -------------------------------------------------------------------

def test_export_dataset_creates_one_parquet_file(
    spark,
    tmp_path,
):
    """
    Verify that the export produces exactly one Parquet file.
    """

    source_path = tmp_path / "source"
    destination_path = tmp_path / "export"

    df = create_test_data(spark)

    # Prepare source Gold data.
    df.write.parquet(str(source_path))

    exported_count = export_dataset(
        spark=spark,
        dataset_name="test_dataset",
        source_path=str(source_path),
        destination_path=str(destination_path),
    )

    parquet_files = list(
        Path(destination_path).glob("*.parquet")
    )

    assert len(parquet_files) == 1
    assert exported_count == 3


def test_export_dataset_preserves_record_count(
    spark,
    tmp_path,
):
    """
    Verify that the exported dataset preserves the source record count.
    """

    source_path = tmp_path / "source"
    destination_path = tmp_path / "export"

    df = create_test_data(spark)

    df.write.parquet(str(source_path))

    source_count = df.count()

    exported_count = export_dataset(
        spark=spark,
        dataset_name="test_dataset",
        source_path=str(source_path),
        destination_path=str(destination_path),
    )

    assert exported_count == source_count


def test_export_dataset_preserves_schema_and_data(
    spark,
    tmp_path,
):
    """
    Verify that the exported Parquet data preserves the source schema
    and business values.
    """

    source_path = tmp_path / "source"
    destination_path = tmp_path / "export"

    source_df = create_test_data(spark)

    source_df.write.parquet(str(source_path))

    export_dataset(
        spark=spark,
        dataset_name="test_dataset",
        source_path=str(source_path),
        destination_path=str(destination_path),
    )

    exported_df = spark.read.parquet(str(destination_path))

    # Compare schemas.
    assert exported_df.schema == source_df.schema

    # Compare records independently of their ordering.
    source_rows = {
        tuple(row)
        for row in source_df.collect()
    }

    exported_rows = {
        tuple(row)
        for row in exported_df.collect()
    }

    assert exported_rows == source_rows