import os

from plugins.docker_utils import (
    execute_in_container,
)

from config.config import (
    POSTGRES_CONTAINER
)

def Create_gold_tables():
    """
    Create the PostgreSQL Gold serving tables by executing the SQL script.

    """

    execute_in_container(
        container_name=POSTGRES_CONTAINER,
        step_name="create_gold_tables_if_not_exist",
        command = [
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            os.getenv("POSTGRES_USER"),
            "-d",
            os.getenv("POSTGRES_DB", "ecommerce"),
            "-f",
            "/docker-entrypoint-initdb.d/create_gold_tables.sql",
        ],
    )
