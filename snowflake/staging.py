import logging
from pathlib import Path

from config.config import (
    SNOWFLAKE_EXPORT_PATH,
    STAGE_NAME,
    FILE_FORMAT_NAME,
    SNOWFLAKE_DATASETS
)

EXPORT_PATH = Path(SNOWFLAKE_EXPORT_PATH)

# -------------------------------------------------------------------
# Logging configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Parquet export validation
# -------------------------------------------------------------------

def get_export_file(dataset_name: str) -> Path:
    """
    Retrieves the single Parquet file for a dataset

    The export directory must contain exactly one Parquet file
    """

    dataset_path = EXPORT_PATH / dataset_name

    if not dataset_path.is_dir():
        raise FileNotFoundError(
            f"Export directory does not exist: {dataset_path}"
        )

    parquet_files = list(dataset_path.glob("*.parquet"))

    if len(parquet_files) != 1:
        raise ValueError(
            f"Expected exactly one Parquet file for {dataset_name}, "
            f"found {len(parquet_files)}."
        )

    parquet_file = parquet_files[0]

    if parquet_file.stat().st_size == 0:
        raise ValueError(
            f"Parquet file is empty: {parquet_file}"
        )

    logger.info(
        "Validated export file for %s: %s",
        dataset_name,
        parquet_file.name,
    )

    return parquet_file


# -------------------------------------------------------------------
# Upload Parquet files to Snowflake stage
# -------------------------------------------------------------------

def upload_to_stage(
    cursor,
    dataset_name: str,
    parquet_file: Path,
    run_id: str,
):
    """
    Uploads one Parquet file to a unique stage directory

    Each execution receives its own directory to avoid conflicts
    with previous Snowflake file-loading history
    """

    stage_path = (
        f"@{STAGE_NAME}/{run_id}/{dataset_name}"
    )

    file_uri = parquet_file.as_uri()

    logger.info(
        "Uploading %s to Snowflake stage: %s",
        dataset_name,
        stage_path,
    )

    cursor.execute(
        f"""
        PUT {file_uri}
        {stage_path}
        AUTO_COMPRESS = FALSE
        OVERWRITE = TRUE
        """
    )

    results = cursor.fetchall()

    for result in results:
        logger.info(
            "Upload result for %s: %s",
            dataset_name,
            result[6] if len(result) > 6 else result,
        )

    logger.info(
        "Upload completed for %s.",
        dataset_name,
    )

    return stage_path


# -------------------------------------------------------------------
# Create temporary table
# -------------------------------------------------------------------

def create_temporary_table(
    cursor,
    dataset_name: str,
):
    """
    Creates a temporary table using the structure of its RAW table

    LIKE copies the table structure without copying its data
    """

    temp_table = SNOWFLAKE_DATASETS[dataset_name]["temp_table"]
    raw_table = SNOWFLAKE_DATASETS[dataset_name]["raw_table"]

    logger.info(
        "Creating temporary table: %s",
        temp_table,
    )

    cursor.execute(
        f"""
        CREATE TEMPORARY TABLE {temp_table}
        LIKE {raw_table}
        """
    )

    logger.info(
        "Temporary table created: %s",
        temp_table,
    )


# -------------------------------------------------------------------
# Load Parquet into temporary table
# -------------------------------------------------------------------

def load_temporary_table(
    cursor,
    dataset_name: str,
    stage_path: str,
) -> int:
    """
    Loads Parquet data from the Snowflake stage into a temporary table.

    Returns the number of records loaded.
    """

    temp_table = SNOWFLAKE_DATASETS[dataset_name]["temp_table"]

    logger.info(
        "Loading %s into %s...",
        dataset_name,
        temp_table,
    )

    cursor.execute(
        f"""
        COPY INTO {temp_table}
        FROM {stage_path}
        FILE_FORMAT = (
            FORMAT_NAME = '{FILE_FORMAT_NAME}'
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
    )

    copy_results = cursor.fetchall()

    loaded_rows = sum(
        row[3] for row in copy_results
    )

    logger.info(
        "%s records loaded into %s.",
        loaded_rows,
        temp_table,
    )

    return loaded_rows
