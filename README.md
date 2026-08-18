# Data Engineering E-commerce Pipeline

Projet end-to-end simulant une plateforme data moderne :
- ingestion (Python/Kafka)
- traitement (PySpark)
- modélisation (dbt)
- orchestration (Airflow)
- visualisation (BI)

## Stack
Python, PySpark, Kafka, PostgreSQL, dbt, Airflow, Docker

## Architecture (WIP)
Ingestion > Kafka > Spark > Data Lake > dbt > BI

## Avancement
- [x] Setup infrastructure (Docker, Kafka, Spark, Postgres)
- [x] Ingestion Kafka
    - Création topic kafka: docker exec kafka kafka-topics --create --topic ecommerce_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    - Voir la liste des topic: docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
    - lancer le main.py
    - Pour vérifier le contenu du topic: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce_events --from-beginning
- [ ] Spark Streaming
- [ ] Data modeling (dbt)
- [ ] Orchestration (Airflow)
- [ ] Dashboard