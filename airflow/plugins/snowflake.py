import docker
from airflow.exceptions import AirflowException

from config.config import (
    SNOWFLAKE_CONTAINER
)


# Execute Snowflake loading pipeline

def load_data_into_snowflake():
    """
    Execute the Snowflake loading pipeline inside the
    dedicated dbt-snowflake container.

    The script:
    - Uploads Parquet files to the Snowflake internal stage.
    - Creates temporary tables.
    - Loads and validates temporary data.
    - Replaces permanent RAW tables transactionally.
    """

    client = docker.from_env()

    try:
        # Retrieve the existing Snowflake loader container.
        container = client.containers.get(SNOWFLAKE_CONTAINER)
        container.reload()

        if container.status != "running":
            raise AirflowException(
                f"dbt-snowflake must be running. "
                f"Current status: {container.status}"
            )

        # Execute the Snowflake loading script.
        command = [
            "python",
            "/opt/snowflake/load_data_into_snowflake.py",
        ]

        print(f"Executing: {' '.join(command)}")

        result = container.exec_run(
            cmd=command,
            stdout=True,
            stderr=True,
            demux=True,
        )

        stdout, stderr = result.output or (None, None)

        # Display standard output in Airflow task logs.
        if stdout:
            print(stdout.decode("utf-8", errors="replace"))

        # Display standard error in Airflow task logs.
        if stderr:
            print(stderr.decode("utf-8", errors="replace"))

        if result.exit_code != 0:
            raise AirflowException(
                "Snowflake loading pipeline failed "
                f"(exit code: {result.exit_code})"
            )

        print(
            "Snowflake RAW tables successfully refreshed."
        )

    finally:
        client.close()

