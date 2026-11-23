from config.config import SPARK_CONTAINER
from docker_utils import execute_in_container


def export_gold_to_snowflake():
    """
    The job reads Gold datasets and generates consolidated
    Parquet exports under /lake/snowflake_export.
    """
    execute_in_container(
        container_name=SPARK_CONTAINER,
        command=[
            "python3",
            "/opt/spark/jobs/export_gold_to_snowflake.py",
        ],
        step_name="spark export",
    )