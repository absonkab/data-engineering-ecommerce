from pyspark.sql import SparkSession
from config.config import (
    POSTGRES_URL,
    POSTGRES_PROPERTIES,
    GOLD_HOURLY_METRICS_TABLE,
)

spark = (
    SparkSession.builder
    .appName('JDBCTest')
    .master('local[1]')
    .getOrCreate()
)

df = spark.read.jdbc(
    url=POSTGRES_URL, 
    table=GOLD_HOURLY_METRICS_TABLE, 
    properties=POSTGRES_PROPERTIES
)

print('JDBC_OK')

df.printSchema()

print('ROWS=', df.count())

spark.stop()