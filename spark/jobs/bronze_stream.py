"""
Premier job Spark Structured Streaming.

Objectif :
    Lire les événements provenant du topic Kafka
    "ecommerce_events" et les afficher dans la console.

Aucune transformation métier n'est effectuée à cette étape.
"""

from pyspark.sql import functions as F

from utils.spark_session import create_spark_session


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "ecommerce_events"


# ---------------------------------------------------------
# Initialisation de Spark
# ---------------------------------------------------------

spark = create_spark_session("EcommerceKafkaStreaming")


# ---------------------------------------------------------
# Lecture du flux Kafka
# ---------------------------------------------------------

events_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


# ---------------------------------------------------------
# Conversion de la valeur Kafka en texte
# ---------------------------------------------------------

events_text_df = events_df.select(
    F.col("value").cast("string").alias("event")
)


# ---------------------------------------------------------
# Affichage du flux dans la console
# ---------------------------------------------------------

query = (
    events_text_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/checkpoints/ecommerce_events")
    .start()
)


# ---------------------------------------------------------
# Maintien du streaming actif
# ---------------------------------------------------------

query.awaitTermination()