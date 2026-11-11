"""
Gold to Snowflake export job.

This job reads the three datasets produced by the Gold layer
and exports them as consolidated Parquet datasets.

Each export contains a single Parquet data file.

Source:
    /lake/gold/

Destination:
    /lake/snowflake_export/

The original Gold datasets are never modified.
"""

import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

from utils.spark_session import create_spark_session

from config.config import (
    GOLD_PATH,
    SNOWFLAKE_EXPORT_PATH
)

# Logging configuration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# Dataset configuration

# Centralizes the source and destination paths of each dataset.
# This avoids duplicating path definitions throughout the job.

EXPORT_DATASETS = {
    "hourly_metrics": {
        "source": os.path.join(GOLD_PATH, "hourly_metrics"),
        "destination": os.path.join(SNOWFLAKE_EXPORT_PATH, "hourly_metrics")
    },
    "product_metrics": {
        "source": os.path.join(GOLD_PATH, "product_metrics"),
        "destination": os.path.join(SNOWFLAKE_EXPORT_PATH, "product_metrics"),
    },
    "user_metrics": {
        "source": os.path.join(GOLD_PATH, "user_metrics"),
        "destination": os.path.join(SNOWFLAKE_EXPORT_PATH, "user_metrics"),
    },
}


# Export function

def export_dataset(
    spark: SparkSession,
    dataset_name: str,
    source_path: str,
    destination_path: str,
) -> int:
    """
    Exports one Gold dataset into a consolidated Parquet dataset.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    dataset_name : str
        Name of the dataset being exported.

    source_path : str
        Source Gold directory.

    destination_path : str
        Destination export directory.

    Returns
    -------
    int
        Number of records exported.

    Raises
    ------
    FileNotFoundError
        If the source directory or Parquet files do not exist.

    ValueError
        If the source dataset is empty or the export is invalid.
    """

    logger.info("Starting export for dataset: %s", dataset_name)

    # Validate source directory

    if not os.path.isdir(source_path):
        raise FileNotFoundError(
            f"Source directory does not exist: {source_path}"
        )

    # Only consider actual Parquet files.
    parquet_files = list(Path(source_path).glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in: {source_path}"
        )

    logger.info(
        "Found %s Parquet files in %s",
        len(parquet_files),
        source_path,
    )

    # Read Gold dataset

    source_df = spark.read.parquet(source_path)

    # Count records to validate the export afterwards.
    source_count = source_df.count()

    if source_count == 0:
        raise ValueError(
            f"Dataset {dataset_name} is empty. Export cancelled."
        )

    logger.info(
        "Dataset %s contains %s records",
        dataset_name,
        source_count,
    )

    # Consolidate and write Parquet

    logger.info(
        "Writing consolidated Parquet export to: %s",
        destination_path,
    )

    (
        source_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(destination_path)
    )

    # Validate generated export

    exported_files = list(
        Path(destination_path).glob("*.parquet")
    )

    if len(exported_files) != 1:
        raise ValueError(
            f"Expected exactly one Parquet file for {dataset_name}, "
            f"but found {len(exported_files)}."
        )

    exported_df = spark.read.parquet(destination_path)

    exported_count = exported_df.count()

    if exported_count != source_count:
        raise ValueError(
            f"Record count mismatch for {dataset_name}: "
            f"source={source_count}, exported={exported_count}"
        )

    logger.info(
        "Export successfully validated: %s records, 1 Parquet file",
        exported_count,
    )

    return exported_count


# Main execution

def main():
    """
    Executes the export of all Gold datasets.
    A single SparkSession is reused for all three exports.
    """

    logger.info("Starting Gold to Snowflake export pipeline")

    spark = create_spark_session(
        "ecommerce-gold-to-snowflake-export"
    )

    try:

        total_records = 0

        # Export each configured dataset.
        for dataset_name, paths in EXPORT_DATASETS.items():

            exported_count = export_dataset(
                spark=spark,
                dataset_name=dataset_name,
                source_path=paths["source"],
                destination_path=paths["destination"],
            )

            total_records += exported_count

        logger.info(
            "All exports completed successfully. Total records: %s",
            total_records,
        )

    finally:

        # Always release Spark resources, even if an export fails.
        spark.stop()

        logger.info("SparkSession stopped")


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    main()