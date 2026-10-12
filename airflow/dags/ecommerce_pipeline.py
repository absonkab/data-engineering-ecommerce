"""
Main orchestration DAG for the ecommerce Data Engineering pipeline.

This DAG orchestrates the batch part of the pipeline:

    Gold metrics -> PostgreSQL Serving -> dbt transformations

Airflow is responsible for orchestration only.
The actual data processing remains implemented in Spark and dbt.
"""

import os
import docker

from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

# Shared Docker network used by the Compose services.
DOCKER_NETWORK = "data-engineering-ecommerce_default"

# Existing dbt image 
DBT_IMAGE = "data-engineering-ecommerce-dbt:latest"


def publish_gold_metrics():
    """
    Execute the three Serving jobs inside the existing spark-job container 
    and its Spark environment.

    """

    client = docker.from_env()

    try:
        # Reuse the existing Spark job container.
        container = client.containers.get("spark-job")
        container.reload()

        if container.status != "running":
            raise AirflowException(
                f"spark-job must be running. Current status: {container.status}"
            )

        # Run the three Serving jobs sequentially.
        commands = [
            [
                "python3",
                "/opt/spark/jobs/serving/publish_hourly_metrics.py",
            ],
            [
                "python3",
                "/opt/spark/jobs/serving/publish_product_metrics.py",
            ],
            [
                "python3",
                "/opt/spark/jobs/serving/publish_user_metrics.py",
            ],
        ]

        for command in commands:
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
                    f"Serving job failed: {' '.join(command)} "
                    f"(exit code: {result.exit_code})"
                )

            print(f"Successfully completed: {' '.join(command)}")

    finally:
        client.close()


with DAG(
    dag_id="ecommerce_pipeline",

    # First logical execution date of the DAG.
    start_date=datetime(2026, 1, 1),

    # We will trigger the pipeline manually during development.
    schedule=None,

    # Do not create historical DAG runs.
    catchup=False,

    description="Orchestration of the ecommerce Data Engineering pipeline.",

    tags=["ecommerce", "spark", "dbt", "production"],
) as dag:

    # Task 1: Check that Gold data is available.
    # Gold is produced by independent streaming services.
    check_gold  = BashOperator(
        task_id="run_gold",
        bash_command="""
            set -e

            for dataset in hourly_metrics product_metrics user_metrics; do
                dataset_path="/lake/gold/${dataset}"

                if [ ! -d "$dataset_path" ]; then
                    echo "ERROR: Gold dataset not found: $dataset_path"
                    exit 1
                fi

                if ! find "$dataset_path" -type f -name '*.parquet' -print -quit | grep -q .; then
                    echo "ERROR: No Parquet files found in $dataset_path"
                    exit 1
                fi

                echo "Gold dataset available: $dataset_path"
            done
        """,
    )


    # Task 2: Publish Gold metrics into PostgreSQL.
    run_serving = PythonOperator(
        task_id="run_serving",
        python_callable=publish_gold_metrics,
    )

    # Task 3: Run dbt transformations inside an isolated Docker container.
    # This Build the analytics models with dbt.
    run_dbt = DockerOperator(
        task_id="run_dbt",

        # Self-contained image containing dbt and the project.
        image=DBT_IMAGE,

        # Run dbt against the existing project.
        command="dbt run",

        # dbt project directory inside the container.
        working_dir="/usr/app/dbt",

        # Tell dbt where profiles.yml is located.
        environment={
            "DBT_PROFILES_DIR": "/usr/app/dbt",

            # PostgreSQL connection settings.
            # These values come from Airflow's environment.
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "ecommerce"),
        },

        # Remove the temporary container after a successful run.
        auto_remove="success",

        # Put the temporary dbt container on the same Docker Compose network as PostgreSQL.
        network_mode=DOCKER_NETWORK,

        # Keep dbt output visible in Airflow logs.
        tty=True,
    )

    # Task dependencies.
    # 1. Verify Gold datasets.
    # 2. Publish them to PostgreSQL.
    # 3. Build analytics models with dbt.
    check_gold >> run_serving >> run_dbt