"""
Simple Airflow DAG used to validate the local Airflow installation.

This DAG is intentionally minimal.
Its goal is to validate the complete Airflow lifecycle:
DAG discovery -> scheduling -> task execution -> logs -> success.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    """
    Simple Python task used to validate task execution.

    In a real pipeline, this function could be replaced by
    a call to a Spark job, dbt command, API, etc.
    """

    print("Hello from Airflow!")
    print("The ecommerce data pipeline orchestration is ready.")


with DAG(
    dag_id="hello_airflow",

    # The first execution date of the DAG.
    start_date=datetime(2026, 1, 1),

    # No automatic scheduling for this first test.
    # We will trigger it manually from the Airflow UI.
    schedule=None,

    # Do not create historical runs between start_date
    # and the current date.
    catchup=False,

    # Description displayed in the Airflow UI.
    description="First Airflow DAG for the ecommerce Data Engineering project.",

    # DAG owner displayed in the Airflow UI.
    tags=["ecommerce", "learning", "test"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )