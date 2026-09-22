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

## Project progress
- [x] Setup infrastructure (Docker, Kafka, Spark, Postgres)
- [x] Kafka Ingestion 
    - Kafka topic creation: docker exec kafka kafka-topics --create --topic ecommerce_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    - List topics: docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
    - Run main.py (Start docker ingestion container)
    - Check a topic content: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce_events --from-beginning
- [x] Spark Streaming
    - [x] parsing JSON + typage + Kafka metadata
    - [x] Bronze Layer
        - Run bronze stream spark-submit: docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/bronze_stream.py
    - [x] Silver Layer
        - Run silver stream spark-submit: docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/silver_stream.py
    - [x] Gold Layer
        - Run hourly metrics stream spark-submit: docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/hourly_metrics.py
        - Run product metrics stream spark-submit: docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/product_metrics.py
        - Run user metrics stream spark-submit: docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/user_metrics.py
    - [x] Pipeline transformation tests
        - test silver_stream : docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_silver.py
        - test bronze_stream : docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_bronze.py
        - test hourly_metrics: docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_hourly_metrics.py
        - test product_metrics: docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_product_metrics.py
        - test user_metrics   : docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_user_metrics.py
        - test all from /tests/unit : docker exec -it spark-job pytest -v /opt/spark/tests/unit
    - [x] Serving Layer
        - [x] create sql gold tables : -> bash: docker exec -i postgres psql -U data_user -d ecommerce < sql/serving/create_gold_tables.sql
                                   -> PowerShell: Get-Content sql/serving/create_gold_tables.sql | docker exec -i postgres psql -U data_user -d ecommerce
        - [x] publish hourly metrics into PostgreSQL. Run with docker exec -it spark-job python3 /opt/spark/jobs/serving/publish_hourly_metrics.py
        - [x] publish product metrics into PostgreSQL. Run with docker exec -it spark-job python3 /opt/spark/jobs/serving/publish_product_metrics.py
- [ ] Data modeling (dbt)
- [ ] Orchestration (Airflow)
- [ ] Dashboard