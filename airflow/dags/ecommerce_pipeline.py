"""
Main orchestration DAG for the ecommerce Data Engineering pipeline.

This DAG orchestrates the batch part of the pipeline:

    Gold metrics -> PostgreSQL Serving -> dbt transformations

Airflow is responsible for orchestration only.
The actual data processing remains implemented in Spark and dbt.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from plugins.postgres import Create_gold_tables
from plugins.serving import publish_gold_metrics
from plugins.dbt import (
    run_dbt_models,
    test_dbt_models,
    generate_dbt_docs
)

from config.config import (
    DBT_CONTAINER,
    DBT_PROJECT_DIR,
    AIRFLOW_POOL_NAME_FOR_SPARK_CONCUR_ACCESS
)


with DAG(
    dag_id="ecommerce_pipeline",

    # First logical execution date of the DAG.
    start_date=datetime(2026, 1, 1),

    # We will trigger the pipeline manually during development.
    # schedule=None,
    # Run the pipeline automatically at the beginning of every hour.
    schedule="0 * * * *",

    # Allow only one active run of this pipeline at a time.
    max_active_runs=1,

    # Do not create historical DAG runs.
    catchup=False,

    # Automatically active the dag: True for no & False for yes
    is_paused_upon_creation=False,

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

    # Task 2: Create the PostgreSQL Gold serving tables.
    check_or_create_tables = PythonOperator(
        task_id="Create_gold_tables_if_not_exist",
        python_callable=Create_gold_tables,
    )


    # Task 3: Publish Gold metrics into PostgreSQL.
    run_serving = PythonOperator(
        task_id="run_serving",
        python_callable=publish_gold_metrics,
        pool=AIRFLOW_POOL_NAME_FOR_SPARK_CONCUR_ACCESS,
    )

    # Task 4: Run dbt transformations inside an isolated Docker container.
    # This Build the analytics models with dbt.
    run_dbt = PythonOperator(
        task_id="dbt_postgres_run",
        python_callable=run_dbt_models,
        op_kwargs={
            "container_name": DBT_CONTAINER,
            "project_dir": DBT_PROJECT_DIR,
        },
    )

    # Task 5: Validate the analytics models.
    test_dbt = PythonOperator(
        task_id="dbt_postgres_test",
        python_callable=test_dbt_models,
        op_kwargs={
            "container_name": DBT_CONTAINER,
            "project_dir": DBT_PROJECT_DIR,
        },
    )

    # Task 6: Generate the dbt documentation.
    dbt_docs = PythonOperator(
        task_id="dbt_postgres_docs",
        python_callable=generate_dbt_docs,
        op_kwargs={
            "container_name": DBT_CONTAINER,
            "project_dir": DBT_PROJECT_DIR,
        },
    )
    # Task dependencies:
    # 1. Verify that Gold datasets are available.
    # 2. Create the PostgreSQL serving tables.
    # 3. Publish Gold metrics to PostgreSQL.
    # 4. Build analytics models with dbt.
    # 5. Validate the dbt models.
    # 6. Generate the dbt documentation.
    check_gold >> check_or_create_tables >> run_serving >> run_dbt >> test_dbt >> dbt_docs