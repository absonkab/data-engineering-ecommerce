"""
Initialisation de la SparkSession utilisée par tous les jobs Spark.

Centraliser cette logique permet de :
- réutiliser la configuration dans tous les jobs ;
- éviter les duplications ;
- faciliter l'évolution de la configuration Spark.
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "EcommerceStreaming") -> SparkSession:
    """
    Crée et configure une SparkSession.

    Parameters
    ----------
    app_name : str
        Nom de l'application Spark affiché dans l'UI.

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