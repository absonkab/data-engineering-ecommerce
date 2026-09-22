from typing import List

from pyspark.sql import DataFrame, SparkSession
from contextlib import contextmanager

# PostgreSQL configuration

from config.config import (
    POSTGRES_URL,
    POSTGRES_PROPERTIES,
)


# PostgreSQL connection

@contextmanager
def get_postgres_connection(spark: SparkSession):
    """
    Open a PostgreSQL JDBC connection and guarantee its closure.
    Parameters:
        - spark: Active SparkSession used to access the JVM JDBC driver.
    Yields:
        - connection:  Active PostgreSQL JDBC connection.
    """

    connection = (
        spark._sc._gateway.jvm.java.sql.DriverManager
        .getConnection(
            POSTGRES_URL,
            POSTGRES_PROPERTIES["user"],
            POSTGRES_PROPERTIES["password"],
        )
    )

    try:
        yield connection

    finally:
        connection.close()


# Write to staging

def write_to_staging(df: DataFrame, staging_table: str, ) -> None:
    """
    Write a DataFrame to a PostgreSQL staging table.

    The staging table is overwritten for every publication.
    """

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", staging_table)
        .option("user", POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver", POSTGRES_PROPERTIES["driver"])
        .mode("overwrite")
        .save()
    )


# Build PostgreSQL UPSERT SQL

def build_upsert_sql(staging_table: str, target_table: str, columns: List[str], conflict_columns: List[str],) -> str:
    """
    Build a PostgreSQL UPSERT statement.
    This function only generates SQL.

    Parameters:
        - staging_table: Temporary PostgreSQL staging table.
        - target_table:Final PostgreSQL Serving table.
        - columns: Columns to insert and update.
        - conflict_columns: Business key used by PostgreSQL ON CONFLICT.

    Returns:
        str: Generated PostgreSQL UPSERT statement.
    """

    # Build the INSERT / SELECT column list.
    column_list = ",\n            ".join(columns)

    # Business key columns must not be updated.
    update_columns = [
        column
        for column in columns
        if column not in conflict_columns
    ]

    # Build the UPDATE clause.
    update_clause = ",\n            ".join(
        f"{column} = EXCLUDED.{column}"
        for column in update_columns
    )

    # Build the ON CONFLICT business key.
    conflict_clause = ", ".join(conflict_columns)

    return f"""
    INSERT INTO {target_table} (
        {column_list}
    )
    SELECT
        {column_list}
    FROM {staging_table}

    ON CONFLICT ({conflict_clause})
    DO UPDATE SET
        {update_clause};
    """


# PostgreSQL UPSERT

def upsert_from_staging(spark: SparkSession, staging_table: str, target_table: str, columns: List[str], conflict_columns: List[str],) -> None:
    """
    Execute a PostgreSQL UPSERT using data from a staging table.

    Parameters:
        - spark: Active SparkSession.
        - staging_table: Temporary PostgreSQL table containing the current publication dataset.
        - target_table: Final PostgreSQL Serving table.
        - columns: Columns to insert/update.
        - conflict_columns: Business key used by PostgreSQL ON CONFLICT.
    """

    # Build the SQL statement.
    upsert_sql = build_upsert_sql(
        staging_table=staging_table,
        target_table=target_table,
        columns=columns,
        conflict_columns=conflict_columns,
    )

    # Open a short-lived connection dedicated to this operation.
    with get_postgres_connection(spark) as connection:

        statement = connection.createStatement()

        try:
            statement.executeUpdate(upsert_sql)

        finally:
            statement.close()


# Cleanup staging

def cleanup_staging(spark: SparkSession, staging_table: str, ) -> None:
    """
    Drop the temporary staging table.
    """

    with get_postgres_connection(spark) as connection:

        statement = connection.createStatement()

        try:
            statement.executeUpdate(
                f"DROP TABLE IF EXISTS {staging_table};"
            )

        finally:
            statement.close()