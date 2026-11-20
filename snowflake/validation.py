import logging


from config.config import SNOWFLAKE_DATASETS

# -------------------------------------------------------------------
# Logging configuration
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Validate temporary table
# -------------------------------------------------------------------

def validate_temporary_table(
    cursor,
    dataset_name: str,
    loaded_rows: int,
):
    """
    Checks that the temporary table contains the expected number
    of records reported by the COPY INTO operation.
    """

    temp_table = SNOWFLAKE_DATASETS[dataset_name]["temp_table"]

    cursor.execute(
        f"SELECT COUNT(*) FROM {temp_table}"
    )

    table_count = cursor.fetchone()[0]

    if table_count != loaded_rows:
        raise ValueError(
            f"Record count mismatch for {dataset_name}: "
            f"COPY loaded={loaded_rows}, "
            f"table count={table_count}"
        )

    if table_count == 0:
        raise ValueError(
            f"No records loaded for {dataset_name}."
        )

    logger.info(
        "Validation successful for %s: %s records.",
        dataset_name,
        table_count,
    )