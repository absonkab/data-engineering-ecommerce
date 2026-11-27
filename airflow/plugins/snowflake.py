from config.config import DBT_SNOWFLAKE_CONTAINER
from docker_utils import execute_in_container


def load_data_into_snowflake():
    """
    The script:
        - Uploads Parquet files to the Snowflake internal stage.
        - Creates temporary tables.
        - Loads and validates temporary data.
        - Replaces permanent RAW tables transactionally.
    """
    execute_in_container(
        container_name=DBT_SNOWFLAKE_CONTAINER,
        command=[
            "python",
            "/opt/snowflake/load_data_into_snowflake.py",
        ],
        step_name="snowflake load",
    )
