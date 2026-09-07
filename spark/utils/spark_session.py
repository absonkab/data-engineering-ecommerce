"""
Initialization of the SparkSession used by all Spark jobs.

Centralizing this logic makes it possible to:
- reuse the configuration across all jobs;
- avoid duplication;
- simplify future changes to the Spark configuration.
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "EcommerceStreaming") -> SparkSession:
    """
    Creates and configures a SparkSession.

    Parameters
    ----------
    app_name : str
        Spark application name displayed on the UI.

    Returns
    -------
    SparkSession
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark:7077")
        # Package nécessaire pour lire Kafka
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark