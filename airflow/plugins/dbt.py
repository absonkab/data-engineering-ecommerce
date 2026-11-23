import docker
from airflow.exceptions import AirflowException

from config.config import (
    SNOWFLAKE_CONTAINER
)


def execute_dbt_command(command: list[str], step_name: str):
    """
    Execute a dbt command inside the dbt-snowflake container.

    Airflow orchestrates the execution through Docker SDK.
    dbt itself remains responsible for running models and tests.
    """

    client = docker.from_env()

    try:
        # Retrieve the dbt-snowflake container.
        container = client.containers.get(SNOWFLAKE_CONTAINER)
        container.reload()

        # Ensure the container is running before executing dbt.
        if container.status != "running":
            raise AirflowException(
                f"Container {SNOWFLAKE_CONTAINER} is not running. "
                f"Current status: {container.status}"
            )

        print(f"Starting dbt step: {step_name}")
        print(f"Executing command: {' '.join(command)}")

        # Execute the dbt command from the dbt project directory.
        result = container.exec_run(
            cmd=command,
            workdir="/usr/app/dbt_snowflake",
            stdout=True,
            stderr=True,
            demux=True
        )

        stdout, stderr = result.output

        # Display standard output.
        if stdout:
            print(stdout.decode("utf-8", errors="replace"))

        # Display standard error.
        if stderr:
            print(stderr.decode("utf-8", errors="replace"))

        # Fail the Airflow task if dbt returns a non-zero exit code.
        if result.exit_code != 0:
            raise AirflowException(
                f"dbt step '{step_name}' failed "
                f"with exit code {result.exit_code}."
            )

        print(f"dbt step '{step_name}' completed successfully.")

    finally:
        client.close()


def run_dbt_models():
    """
    Build all dbt models in Snowflake.
    """

    execute_dbt_command(
        command=["dbt", "run", "--target", "dev"],
        step_name="dbt run"
    )


def test_dbt_models():
    """
    Execute all dbt data quality tests.
    """

    execute_dbt_command(
        command=["dbt", "test", "--target", "dev"],
        step_name="dbt test"
    )


def generate_dbt_docs():
    """
    Generate dbt documentation artifacts.
    """

    execute_dbt_command(
        command=["dbt", "docs", "generate", "--target", "dev"],
        step_name="dbt docs generate"
    )
