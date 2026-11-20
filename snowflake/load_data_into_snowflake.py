"""
Snowflake loading job

This job:
    - Connects to Snowflake using key-pair authentication
    - Uploads Gold Parquet exports to an internal stage.
    - Creates temporary tables based on the RAW table structures
    - Loads Parquet files into temporary tables
    - Validates the number of loaded records
    - refresh the raw tables

Source:
    /lake/snowflake_export/

Destination:
    Snowflake internal stage and temporary tables
"""

import logging
import uuid

from config.config import SNOWFLAKE_DATASETS

from snowflake_connection import create_snowflake_connection
from staging import (
    get_export_file,
    upload_to_stage,
    create_temporary_table,
    load_temporary_table,
)
from validation import validate_temporary_table
from raw_refresh import refresh_raw_tables


# -------------------------------------------------------------------
# Logging configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Main execution
# -------------------------------------------------------------------

def main():
    """
    Executes the Snowflake staging and loading process.

    Permanent RAW tables remain untouched.
    """

    logger.info(
        "Starting Snowflake loading pipeline."
    )

    # Unique identifier for this execution.
    run_id = uuid.uuid4().hex

    logger.info(
        "Snowflake execution ID: %s",
        run_id,
    )

    connection = None

    try:

        # Establish Snowflake connection.
        connection = create_snowflake_connection()

        cursor = connection.cursor()

        try:

            total_records = 0

            # Process each Gold dataset.
            for dataset_name in SNOWFLAKE_DATASETS:

                logger.info(
                    "Processing dataset: %s",
                    dataset_name,
                )

                # Validate local Parquet export.
                parquet_file = get_export_file(
                    dataset_name
                )

                # Upload Parquet to a unique stage directory.
                stage_path = upload_to_stage(
                    cursor=cursor,
                    dataset_name=dataset_name,
                    parquet_file=parquet_file,
                    run_id=run_id,
                )

                # Create temporary table.
                create_temporary_table(
                    cursor=cursor,
                    dataset_name=dataset_name,
                )

                # Load staged Parquet into temporary table.
                loaded_rows = load_temporary_table(
                    cursor=cursor,
                    dataset_name=dataset_name,
                    stage_path=stage_path,
                )

                # Validate loaded data.
                validate_temporary_table(
                    cursor=cursor,
                    dataset_name=dataset_name,
                    loaded_rows=loaded_rows,
                )

                total_records += loaded_rows

            logger.info(
                "All temporary tables loaded successfully."
            )

            logger.info(
                "Total records loaded: %s",
                total_records,
            )

            #  Refresh permanent RAW tables only after
            # all temporary tables have been successfully validated.
            refresh_raw_tables(connection, cursor)

            logging.info(
                "All datasets successfully loaded into Snowflake RAW."
            )

        finally:

            cursor.close()

    except Exception:

        logger.exception(
            "Snowflake loading pipeline failed."
        )

        raise

    finally:

        if connection is not None:
            connection.close()

            logger.info(
                "Snowflake connection closed."
            )


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    main()