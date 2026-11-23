"""
Snowflake export orchestration DAG.

This DAG orchestrates the export of Gold datasets
to consolidated Parquet files for Snowflake ingestion.

Pipeline:
    Gold datasets -> Spark export -> Parquet validation

Airflow is responsible for orchestration only.
The actual data processing remains implemented in Spark.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# plugins functions
from plugins.spark import export_gold_to_snowflake
from plugins.snowflake import load_data_into_snowflake
from plugins.validation import (
    check_spark_worker,
    validate_snowflake_exports
)
from plugins.dbt import (
    run_dbt_models,
    test_dbt_models,
    generate_dbt_docs
)


# Configuration

# Local export directory shared between Docker containers
from config.config import (
    GOLD_PATH,
    GOLD_DATASETS
)

# DAG definition

with DAG(
    dag_id="snowflake_export_pipeline",

    # First logical execution date
    start_date=datetime(2026, 1, 1),

    # Manual execution during development
    schedule=None,
    # Run the pipeline automatically at the beginning of every hour.
    # schedule="0 * * * *",

    # Only one active execution at a time
    max_active_runs=1,

    # Do not create historical DAG runs
    catchup=False,

    description=(
        "Export Gold datasets to Parquet and load them "
        "into Snowflake RAW tables."
    ),

    tags=[
        "ecommerce",
        "spark",
        "snowflake",
        "export",
        "ingestion",
    ],

) as dag:

    # Task 1: Check Gold datasets availability

    # check_gold = PythonOperator(
    #     task_id="check_gold_datasets",
    #     python_callable=check_gold_datasets,
    # )

    check_gold = BashOperator(
        task_id="check_gold_datasets",

        bash_command=f"""
            set -e

            for dataset in {" ".join(GOLD_DATASETS)}; do

                dataset_path="{GOLD_PATH}/${{dataset}}"

                if [ ! -d "$dataset_path" ]; then
                    echo "ERROR: Gold dataset not found: $dataset_path"
                    exit 1
                fi

                if ! find "$dataset_path" \
                    -type f -name '*.parquet' \
                    -print -quit | grep -q .; then

                    echo "ERROR: No Parquet files found in $dataset_path"
                    exit 1
                fi

                echo "Gold dataset available: $dataset_path"

            done
        """,
    )

    # Task 2: Check Spark Worker availability

    check_worker = PythonOperator(
        task_id="check_spark_worker",
        python_callable=check_spark_worker,
    )
    
    # Task 3: Execute Spark export

    run_spark_export = PythonOperator(
        task_id="run_spark_export",
        python_callable=export_gold_to_snowflake,
    )

    # Task 4: Validate generated exports

    validate_exports = PythonOperator(
        task_id="validate_snowflake_exports",
        python_callable=validate_snowflake_exports,
    )

    # Task 5: Load validated exports into Snowflake RAW

    load_snowflake = PythonOperator(
        task_id="load_data_into_snowflake",
        python_callable=load_data_into_snowflake,
    )

    # Task 6: Run dbt models
    run_dbt = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_models
    )

    # Task 7: Run dbt tests
    test_dbt = PythonOperator(
        task_id="test_dbt_models",
        python_callable=test_dbt_models
    )

    # Task 8: generate dbt documentation
    generate_docs = PythonOperator(
        task_id="generate_dbt_docs",
        python_callable=generate_dbt_docs
    )

    # Task dependencies
    # 1. Check Gold datasets availability
    # 2. Check Spark Worker availability
    # 3. Execute Spark export
    # 4. Validate generated exports
    # 5. Load data into snowflake stage
    # 6. Run dbt models
    # 7. Run dbt tests
    # 8. generate dbt documentation
    check_gold >> check_worker >> run_spark_export >> validate_exports >> load_snowflake >> run_dbt >> test_dbt >> generate_docs