# Data Engineering E-commerce Pipeline

End-to-end project simulating a modern data platform:
- ingestion (Python/Kafka)
- processing (PySpark)
- modeling (dbt)
- orchestration (Airflow)
- visualization (BI)

## Stack
Python, PySpark, Kafka, PostgreSQL, dbt, Airflow, Docker

## Architecture (WIP)
Ingestion > Kafka > Spark > Data Lake > dbt > BI

## Avancement
- [x] Setup infrastructure (Docker, Kafka, Spark, Postgres)
- [x] Kafka Ingestion 
    - Kafka topic creation: docker exec kafka kafka-topics --create --topic ecommerce_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    - List topics: docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
    - Run main.py (Start docker ingestion container)
    - Check a topic content: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce_events --from-beginning
- [x] Spark Streaming
    - [x] parsing JSON + typage + Kafka metadata
    - [x] Bronze Layer
        - Run bronze stream spark-submit: /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/bronze_stream.py
    - [x] Silver Layer
        - Run silver stream spark-submit: /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/silver_stream.py
    - [ ] Gold Layer
- [ ] Data modeling (dbt)
- [ ] Orchestration (Airflow)
- [ ] Dashboard