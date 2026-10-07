"""
Main orchestration DAG for the ecommerce Data Engineering pipeline.

This DAG orchestrates the batch part of the pipeline:

    Gold metrics -> PostgreSQL Serving -> dbt transformations

Airflow is responsible for orchestration only.
The actual data processing remains implemented in Spark and dbt.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator


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

    # ---------------------------------------------------------
    # Task 1: Generate Gold metrics with Spark.
    # ---------------------------------------------------------
    run_gold = BashOperator(
        task_id="run_gold",

        # We will replace this command with our real Spark command
        # after validating that Airflow can execute commands
        # inside the appropriate container.
        bash_command="echo 'Running Gold metrics'",
    )

    # ---------------------------------------------------------
    # Task 2: Load Gold metrics into PostgreSQL.
    # ---------------------------------------------------------
    run_serving = BashOperator(
        task_id="run_serving",

        # Temporary command used for the first orchestration test.
        bash_command="echo 'Running Serving layer'",
    )

    # -----------------------------------------------------------------------------
    # Task 3: Run dbt transformations inside an isolated Docker container.
    # -----------------------------------------------------------------------------
    run_dbt = DockerOperator(
        task_id="run_dbt",

        # Self-contained image containing dbt and the project.
        image="data-engineering-ecommerce-dbt:latest",

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
        network_mode="data-engineering-ecommerce_default",

        # Keep dbt output visible in Airflow logs.
        tty=True,
    )

    # Task dependencies.
    # Airflow will execute: run_gold → run_serving → run_dbt
    run_gold >> run_serving >> run_dbt