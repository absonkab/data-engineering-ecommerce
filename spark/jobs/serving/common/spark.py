from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession for a Serving job.
    """

    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )