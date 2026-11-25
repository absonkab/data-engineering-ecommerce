"""
Initialization of the SparkSession used by all Spark jobs.

Centralizing this logic makes it possible to:
- reuse the configuration across all jobs;
- avoid duplication;
- configure resources according to the workload.
- simplify future changes to the Spark configuration.
"""

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "EcommerceStreaming",
    max_cores: int | None = None,
    executor_cores: int | None = None,
    executor_memory: str | None = None,
) -> SparkSession:
    """
    Creates and configures a SparkSession.

    Parameters
    ----------
    app_name : str
        Application name displayed on the Spark UI.

    max_cores : int, optional
        Maximum number of cores allocated to the application.

    executor_cores : int, optional
        Number of cores per executor.

    executor_memory : str, optional
        Executor memory (e.g. "1g", "2g").

    Returns
    -------
    SparkSession
    """

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
    )

    # Apply resource configuration only when explicitly provided.
    if max_cores is not None:
        builder = builder.config("spark.cores.max", str(max_cores))

    if executor_cores is not None:
        builder = builder.config("spark.executor.cores", str(executor_cores))

    if executor_memory is not None:
        builder = builder.config("spark.executor.memory", executor_memory)

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark