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
# get a table row count
# -------------------------------------------------------------------

def get_table_row_count(cursor, table_name):
    """
    Return the number of rows in a Snowflake table.
    """

    query = f"""
        SELECT COUNT(*)
        FROM ECOMMERCE_DB.RAW.{table_name}
    """

    cursor.execute(query)

    return cursor.fetchone()[0]

# -------------------------------------------------------------------
# Refresh raw tables
# -------------------------------------------------------------------

def refresh_raw_tables(connection, cursor):
    """
    Replace all permanent RAW tables with the validated temporary tables.

    The three replacements are executed in a single transaction.

    If any operation fails, the transaction is rolled back,
    preserving the previous RAW data.
    """

    logging.info("Starting RAW tables Full Refresh.")

    transaction_started = False

    try:
        # Retrieve expected row counts from temporary tables
        expected_counts = {}

        for dataset_name, config in SNOWFLAKE_DATASETS.items():

            temp_table = config["temp_table"]

            row_count = get_table_row_count(cursor, temp_table)

            if row_count == 0:
                raise ValueError(
                    f"Temporary table {temp_table} is empty. "
                    "RAW refresh cancelled."
                )

            expected_counts[dataset_name] = row_count

            logging.info(
                "%s: %s rows ready for RAW refresh.",
                dataset_name,
                row_count
            )

        # Start a single transaction

        cursor.execute("BEGIN TRANSACTION")

        transaction_started = True

        logging.info("Snowflake transaction started.")

        # Delete existing RAW data

        for dataset_name, config in SNOWFLAKE_DATASETS.items():

            raw_table = config["raw_table"]

            cursor.execute(
                f"""
                DELETE FROM ECOMMERCE_DB.RAW.{raw_table}
                """
            )

            logging.info(
                "Existing data deleted from %s.",
                raw_table
            )

        # Insert validated data into RAW tables

        for dataset_name, config in SNOWFLAKE_DATASETS.items():

            raw_table = config["raw_table"]
            temp_table = config["temp_table"]

            cursor.execute(
                f"""
                INSERT INTO ECOMMERCE_DB.RAW.{raw_table}
                SELECT *
                FROM ECOMMERCE_DB.RAW.{temp_table}
                """
            )

            logging.info(
                "Data inserted into %s.",
                raw_table
            )

        # Validate permanent RAW tables before committing

        for dataset_name, config in SNOWFLAKE_DATASETS.items():

            raw_table = config["raw_table"]

            actual_count = get_table_row_count(
                cursor,
                raw_table
            )

            expected_count = expected_counts[dataset_name]

            if actual_count != expected_count:
                raise ValueError(
                    f"Row count mismatch for {raw_table}: "
                    f"expected {expected_count}, "
                    f"found {actual_count}."
                )

            logging.info(
                "%s validation successful: %s rows.",
                raw_table,
                actual_count
            )

        # Commit the transaction

        cursor.execute("COMMIT")

        transaction_started = False

        logging.info(
            "RAW tables Full Refresh completed successfully."
        )

    except Exception:

        # Roll back if the transaction has started

        if transaction_started:

            logging.exception(
                "RAW refresh failed. Rolling back transaction."
            )

            connection.rollback()

            logging.info(
                "Transaction rolled back. Previous RAW data preserved."
            )

        raise
