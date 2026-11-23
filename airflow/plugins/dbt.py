from config.config import (
    SNOWFLAKE_CONTAINER,
    SNOWFLAKE_DBT_PROJECT_DIR
)

from docker_utils import execute_in_container


def run_dbt_models():
    """
    Build all dbt models in Snowflake.
    """
    execute_in_container(
        container_name=SNOWFLAKE_CONTAINER,
        command=[
            "dbt",
            "run",
            "--target",
            "dev",
        ],
        step_name="dbt run",
        workdir=SNOWFLAKE_DBT_PROJECT_DIR,
    )

def test_dbt_models():
    """
    Execute all dbt data quality tests.
    """
    execute_in_container(
        container_name=SNOWFLAKE_CONTAINER,
        command=[
            "dbt",
            "test",
            "--target",
            "dev",
        ],
        step_name="dbt test",
        workdir=SNOWFLAKE_DBT_PROJECT_DIR,
    )

def generate_dbt_docs():
    """
    Generate dbt documentation artifacts.
    """

    execute_in_container(
        container_name=SNOWFLAKE_CONTAINER,
        command=[
            "dbt",
            "docs",
            "generate",
            "--target",
            "dev",
        ],
        step_name="dbt docs generate",
        workdir=SNOWFLAKE_DBT_PROJECT_DIR,
    )
