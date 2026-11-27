from plugins.docker_utils import (
    execute_in_container
)

from config.config import (
    SPARK_CONTAINER
)

def publish_hourly_metrics():

    execute_in_container(
        container_name=SPARK_CONTAINER,
        step_name="publish_hourly_metrics",
        command=[
            "python3",
            "/opt/spark/jobs/serving/publish_hourly_metrics.py",
        ]
    )


def publish_product_metrics():

    execute_in_container(
        container_name=SPARK_CONTAINER,
        step_name="publish_product_metrics",
        command=[
            "python3",
            "/opt/spark/jobs/serving/publish_product_metrics.py",
        ],
    )


def publish_user_metrics():

    execute_in_container(
        container_name=SPARK_CONTAINER,
        step_name="publish_user_metrics",
        command=[
            "python3",
            "/opt/spark/jobs/serving/publish_user_metrics.py",
        ],
    )



def publish_gold_metrics():
    """
    Execute the three Serving jobs inside the existing spark-job container 
    and its Spark environment.

    """
    publish_hourly_metrics()
    publish_product_metrics()
    publish_user_metrics()


# Application entry point

if __name__ == "__main__":
    publish_gold_metrics()