import docker
from airflow.exceptions import AirflowException

from config.config import (
    SPARK_CONTAINER
)

# Execute Spark export job

def export_gold_to_snowflake():
    """
    Execute the Spark export job inside the existing spark-job
    container.

    The job reads Gold datasets and generates consolidated
    Parquet exports under /lake/snowflake_export.
    """

    client = docker.from_env()

    try:
        # Reuse the existing Spark job container.
        container = client.containers.get(SPARK_CONTAINER)
        container.reload()

        if container.status != "running":
            raise AirflowException(
                f"spark-job must be running. "
                f"Current status: {container.status}"
            )

        # Execute the Spark export script
        command = [
            "python3",
            "/opt/spark/jobs/export_gold_to_snowflake.py",
        ]

        print(f"Executing: {' '.join(command)}")

        result = container.exec_run(
            cmd=command,
            stdout=True,
            stderr=True,
            demux=True,
        )

        stdout, stderr = result.output or (None, None)

        if stdout:
            print(stdout.decode("utf-8", errors="replace"))

        if stderr:
            print(stderr.decode("utf-8", errors="replace"))

        if result.exit_code != 0:
            raise AirflowException(
                "Snowflake export job failed "
                f"(exit code: {result.exit_code})"
            )

        print("Gold datasets successfully exported.")

    finally:
        client.close()

