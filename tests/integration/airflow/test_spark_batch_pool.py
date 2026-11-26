from datetime import datetime
import time

from airflow import DAG
from airflow.operators.python import PythonOperator

"""
To test this pool: 
Move this file into airflow dags folder
Open the Airflow interface:

http://localhost:8082
1. Search for the DAG `test_spark_batch_pool`.
2. Enable it if necessary.
3. Click on "Trigger DAG".
4. Open the execution details (Grid view).
What we should observe:
The two tasks are independent, but they share a pool with only a single slot.

| Task            | Expected behavior                       |
|-----------------|-----------------------------------------|
| `spark_batch_a` | Transitions to Running                  |
| `spark_batch_b` | Remains Queued                          |
| `spark_batch_a` | Transitions to Success after 30 seconds |
| `spark_batch_b` | Subsequently transitions to Running     |
| `spark_batch_b` | Transitions to Success after 30 seconds |

Total expected duration: approximately 60 seconds, excluding planning time.
"""


def simulate_spark_batch(task_name):
    start_time = datetime.now()

    print(f"[{task_name}] STARTED at {start_time}")

    # Simulate a Spark batch job
    time.sleep(30)

    end_time = datetime.now()

    print(f"[{task_name}] FINISHED at {end_time}")


with DAG(
    dag_id="test_spark_batch_pool",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["integration-test", "airflow-pool"],
) as dag:

    task_a = PythonOperator(
        task_id="spark_batch_a",
        python_callable=simulate_spark_batch,
        op_kwargs={"task_name": "SPARK_BATCH_A"},
        pool="spark_batch_pool",
    )

    task_b = PythonOperator(
        task_id="spark_batch_b",
        python_callable=simulate_spark_batch,
        op_kwargs={"task_name": "SPARK_BATCH_B"},
        pool="spark_batch_pool",
    )