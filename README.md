# Real-Time E-commerce Data Platform

An end-to-end Data Engineering project that simulates e-commerce events, processes streaming data with Apache Spark, stores analytical metrics in PostgreSQL, and builds tested data models with dbt.

The platform combines real-time processing with scheduled orchestration to demonstrate a reproducible data pipeline using Docker Compose.

## Overview

This project implements a complete data pipeline, from event ingestion to analytics-ready data.

The main objectives are to:

* Ingest simulated e-commerce events using Python and Apache Kafka.
* Process streaming data with Spark Structured Streaming.
* Organize processed data into Bronze, Silver, and Gold layers.
* Store Gold metrics in PostgreSQL for analytical use.
* Build analytical models with dbt.
* Validate data transformations with automated dbt tests.
* Orchestrate the pipeline with Apache Airflow.
* Run the orchestration DAG automatically every hour.

The goal is to build a reproducible data platform using technologies commonly found in modern data engineering environments.


## Technology Stack

| Technology              | Purpose                                                    |
| ----------------------- | -----------------------------------------------------------|
| Python                  | Event generation, ingestion, pipeline and serving scripts  |
| Apache Kafka            | Event streaming and message transport                      |
| Apache Spark 3.5.1      | Distributed data processing and Structured Streaming       |
| Data Lake               | Storage of Bronze, Silver, and Gold datasets               |
| PostgreSQL 15           | Storage of serving-ready analytical metrics                |
| dbt Core                | SQL transformations and data quality tests                 |
| Apache Airflow 2.10.5   | Pipeline orchestration and scheduling                      |
| Docker & Docker Compose | Containerization, local environment and service management |
| pgAdmin                 | PostgreSQL database administration                         |


## Architecture
Ingestion > Kafka > Spark > Data Lake > dbt > BI

<details>
<summary>Click to see the full architecture</summary>

```text
                  +------------------+
                  | Python Producer  |
                  +------------------+
                           |
                           |
                           │
       ┌───────────────────┴───────────────────┐
       │                                       │
NORMAL EVENTS                                ANOMALY EVENTS
       |                                       │
       |      ┌────────────────┬───────────────┼───────────────┐
       |      ▼                ▼               ▼               ▼
       |      MISSING        INVALID         LATE          DUPLICATE
       |      │                │               │               |
       |      └────────────────┴───────┬───────┴───────────────┘
       |                               │
       |                               |
       └────────────────────┬──────────┘
                            |
                     ┌──────────────┐
                     │     Kafka    │
                     └──────┬───────┘
                            │
                     ┌───────────────┐
                     │Spark Streaming│
                     └───────┬───────┘
                             ▼
                     ┌──────────────┐
                     │    BRONZE    │
                     │  Raw + Kafka │
                     │   metadata   │
                     └──────┬───────┘
                            │
                            ▼
                       ┌───────────────┐
                       │Spark Streaming│
                       └───────┬───────┘
                               │
                     nettoyage / normalisation
                               │
                     ┌─────────┴─────────┐
                     │                   │
                     ▼                   ▼
              VALID EVENTS        INVALID EVENTS
                     │                   │
              watermark 10 min           │
                     │                   │
              dedup event_id             │
                     │                   │
                     ▼                   ▼
                  SILVER             QUARANTINE
                     │
         ┌───────────┼───────────┐
         ▼           ▼           ▼
       Hourly      Product       User
        Gold         Gold         Gold
         │           │           │
         └───────────┼───────────┘
                     │
                     ▼
                Spark Publisher
                     │
                     ▼
                 PostgreSQL
                     │
          PostgreSQL / serving
                     │
                     ├── gold_hourly_metrics  ───┐
                     ├── gold_product_metrics  ──┼──→ dbt sources
                     └── gold_user_metrics  ─────┘
                                                 │
                                                 ▼
                                            ┌─────────┐
                                            │ Sources │
                                            └────┬────┘
                                                 │
                                                 ▼
                                            ┌─────────┐
                                            │ Staging │
                                            └────┬────┘
                                                 │
                                                 ├── stg_hourly_metrics     VIEW
                                                 ├── stg_product_metrics    VIEW
                                                 └── stg_user_metrics       VIEW
                                                 |
                                                 ▼
                                          ┌──────────────┐
                                          │ Intermediate │
                                          └──────┬───────┘
                                                 │
                                                 ├── int_hourly_metrics     VIEW
                                                 ├── int_product_metrics    VIEW
                                                 └── int_user_metrics       VIEW
                                                 │
                                                 ▼
                                             ┌───────┐
                                             │ Marts │
                                             └───────┘
                                                 │
                                                 ├── mart_hourly_metrics     TABLE
                                                 ├── mart_product_metrics    TABLE
                                                 └── mart_user_metrics       TABLE
                                                 |
                                                 ├── tests
                                                 └── documentation
                                                 │
                                                 ▼
                                          ┌──────────────┐
                                          │   Dashboard  │
                                          └──────────────┘
                                                 ▲
                                                 │
                                          ┌──────┴───────┐
                                          │   Airflow    │
                                          │ Orchestration│
                                          └──────────────┘
                                                 │
                                     ┌───────────┴───────────┐
                               DockerOperator           PythonOperator
                                     |                       |
                                     ▼                       ▼
                              dbt container             Spark container
                                     │                       │
                                     ▼                       ▼
                             dbt run / dbt test      Gold serving scripts

```
</details>


## Data Pipeline

### 1. Event Ingestion

A Python producer generates simulated e-commerce events and publishes them to the Kafka topic `ecommerce_events`.

Kafka decouples event generation from downstream processing.

### 2. Streaming Processing

Spark Structured Streaming consumes events from Kafka and processes them through three data layers.

* **Bronze:** initial ingestion of streaming events.
* **Silver:** data cleaning and transformation.
* **Gold:** analytical metrics prepared for downstream consumption.

The Gold datasets are stored as Parquet files in the local data lake.

### 3. Gold Metrics

The Gold layer contains three analytical datasets:

* `hourly_metrics`
* `product_metrics`
* `user_metrics`

These datasets are published to three PostgreSQL serving tables:

* `gold_hourly_metrics`
* `gold_product_metrics`
* `gold_user_metrics`

The serving scripts use PostgreSQL upserts to insert new metrics and update existing records when their keys match.

### 4. Analytics with dbt

dbt builds analytical models from the serving data in PostgreSQL.

The project includes automated dbt tests to validate the resulting models.

The current project validation completed successfully with **54 dbt tests passing**.

### 5. Orchestration with Airflow

The Airflow DAG is named `ecommerce_pipeline`.

It runs automatically at the beginning of every hour and can also be triggered manually.

The DAG executes five tasks in sequence:

| Order | Task                     | Responsibility                                                                           |
| ----- | ------------------------ | ---------------------------------------------------------------------------------------- |
| 1     | `run_gold`               | Checks that the three Gold datasets contain Parquet files.                               |
| 2     | `check_or_create_tables` | Executes the SQL script that creates the PostgreSQL serving tables if they do not exist. |
| 3     | `run_serving`            | Publishes Gold metrics to PostgreSQL.                                                    |
| 4     | `run_dbt`                | Builds the analytical models.                                                            |
| 5     | `test_dbt`               | Runs the dbt tests.                                                                      |

The DAG uses `catchup=False` to avoid creating historical runs and `max_active_runs=1` to prevent overlapping active runs.

**Important:** Airflow orchestrates the batch and analytics steps. The Kafka ingestion and Spark streaming applications operate independently.


## Data Quality

The dbt test suite contains 54 data quality tests.

Latest successful execution:
```text
PASS=54 WARN=0 ERROR=0 SKIP=0 TOTAL=54
```
These results demonstrate that all 54 tests passed during the recorded execution. They do not guarantee that every future dataset will pass the same tests.


## Project Structure

The main project components are organized as follows:
<details>
<summary>Click to see</summary>

```text
data-engineering-ecommerce/
|       
+---airflow
|   |
|   +---dags
|   |           
|   \---logs
|       
+---config
|       
+---dbt
|   |   
|   +---logs
|   |       
|   +---models
|   |   |   
|   |   +---intermediate
|   |   |       
|   |   +---marts
|   |   |       
|   |   \---staging
|   |           
|   +---target
|   |                         
|   \---tests
|           
+---ingestion
|   |   
|   +---producer
|           
+---lake
|   |
|   +---bronze
|   |
|   +---checkpoints
|   |                       
|   +---gold
|   |   +---hourly_metrics
|   |   |           
|   |   +---product_metrics
|   |   |           
|   |   \---user_metrics
|   |               
|   +---quarantine
|   | 
|   \---silver
|  
+---spark
|   |   
|   +---jobs
|   |   |   
|   |   +---serving
|   |   |   |   
|   |   |   \---common
|   |           
|   +---schemas
|   |       
|   +---utils
|           
+---sql
|   \---serving
|           
\---tests
|    +---integration
|    |       
|    \---unit
|
|.env
|.env.example
|.gitignore
|docker-compose.yml
|README.md
```
</details></br>

The exact locations of the Spark jobs and dbt models depend on their implementation in the repository. The `lake/` directory is used for the Parquet data lake and is mounted into the relevant containers.

## Prerequisites

Before starting, install:

* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* Docker Compose, included with current Docker Desktop installations.
* Git, if you want to clone the repository.

The project is designed to run locally with Docker Compose.
Make sure Docker has enough CPU and memory resources to run the services concurrently.

## Getting Started
The project uses Docker Compose to manage its services.
Build the required images and start the infrastructure.

### 1. Clone the repository

```bash
git clone https://github.com/absonkab/data-engineering-ecommerce.git
cd data-engineering-ecommerce
```

### 2. Configure environment variables

Create a local `.env` file from the example:

**Windows PowerShell**

```powershell
Copy-Item .env.example .env
```

**Linux / macOS**

```bash
cp .env.example .env
```

Review `.env` and configure the required values, including:

* PostgreSQL credentials and database name.
* Airflow credentials.
* Service ports.
* Spark resource settings, if applicable.

Keep `.env` local. Do not commit passwords or other secrets to GitHub.

#### Spark Resource Allocation

The project runs five Spark Structured Streaming applications concurrently:

* Bronze Streaming
* Silver Streaming
* Gold Hourly Metrics
* Gold Product Metrics
* Gold User Metrics

The default local configuration allocates:

- **5 CPU cores** to the Spark worker
- **6 GB RAM** to the Spark worker
- **1 CPU core maximum** per streaming application

These settings help distribute resources across the streaming applications in a local development environment.

Adjust the values in `.env` if your machine has different resource limits.

### 3. Validate the Docker Compose configuration

```bash
docker compose config --quiet
```

If the command completes without an error, the Compose configuration is valid.

### 4. Build and start the services

```bash
docker compose up -d --build
```

Docker Compose starts the services and their configured dependencies.

The `kafka-init` service waits for Kafka to become available and creates the `ecommerce_events` topic if it does not already exist.

The PostgreSQL service mounts `sql/serving` into `/docker-entrypoint-initdb.d`. PostgreSQL executes initialization scripts from this directory when initializing a new, empty data directory.

For an existing database, the Airflow `create_gold_tables` task executes `create_gold_tables.sql` before publishing the metrics.

You can also start the services one by one:

```bash
docker compose up -d --build service_name
```
Refer to the Docker Compose configuration for the exact service names and startup commands.


### 5. Check the running containers
Verify that Kafka, PostgreSQL, Spark, and Airflow are running and healthy where healthchecks are configured.

```bash
docker compose ps
```

To inspect logs:

```bash
docker compose logs --tail=100
```

To inspect the Kafka initialization and event ingestion:

```bash
docker compose logs --tail=50 kafka-init ingestion
```

To list Kafka topics:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

The topic `ecommerce_events` should be present.

Confirm that the ingestion service produces events and that the Spark Structured Streaming applications process them.

Check that the Bronze, Silver, and Gold datasets are being generated.

### 6. Access the interfaces

When the corresponding services are running, access the interfaces using the configured local ports.

| Service           | Local address         |
| ----------------- | --------------------- |
| Spark Master UI   | http://localhost:8080 |
| pgAdmin           | http://localhost:5050 |
| dbt documentation | http://localhost:8081 |
| Airflow           | http://localhost:8082 |

The available interfaces and ports depend on the Docker Compose configuration.

Use the credentials configured in `.env` where authentication is required.


## Running the Pipeline

### Automatic execution

Once Airflow is running and the `ecommerce_pipeline` DAG is enabled, Airflow schedules the DAG hourly.

The schedule is configured as:

```python
schedule="0 * * * *"
```

This means the DAG is scheduled at minute zero of every hour, according to Airflow's configured timezone.

### Manual execution

To test the pipeline, open the Airflow interface:

1. Navigate to `ecommerce_pipeline`.
2. Ensure the DAG is not paused.
3. Trigger a new DAG run.
4. Open the task graph.
5. Verify that all five tasks complete successfully.

Manual triggering remains useful for development and troubleshooting.

### Verify PostgreSQL serving tables

The following command lists the three Gold serving tables:

```bash
docker exec postgres psql -U data_user -d ecommerce -c "\dt gold_*"
```

Replace the username and database name if your `.env` uses different values.

Expected tables:

```text
gold_hourly_metrics
gold_product_metrics
gold_user_metrics
```

### Verify the Gold data lake

The Gold datasets should contain Parquet files under:

```text
lake/gold/hourly_metrics/
lake/gold/product_metrics/
lake/gold/user_metrics/
```

The Airflow `run_gold` task checks that each directory exists and contains at least one Parquet file before continuing.

## Configuration and Data Persistence

The project uses Docker Compose volumes to share data and configuration between services.

The Gold data lake is mounted into the relevant containers at `/lake`. Airflow uses the same mount to check the Gold datasets.

The PostgreSQL data directory is persisted in a Docker volume.

**Do not use `docker compose down -v` unless you intentionally want to remove the Compose-managed volumes.** Removing database volumes can delete persisted data.

For a normal shutdown, use:

```bash
docker compose down
```

To restart the services:

```bash
docker compose up -d
```

## Troubleshooting

### Kafka topic is missing

Check that Kafka is running:

```bash
docker compose ps kafka
```

Inspect the initialization logs:

```bash
docker compose logs kafka-init
```

Then list the topics:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

### Gold datasets are missing

Check the Spark streaming services:

```bash
docker compose ps
```

Inspect the logs of the relevant streaming applications:

```bash
docker compose logs --tail=100 bronze-stream silver-stream gold-hourly gold-product gold-user
```

### PostgreSQL serving tables are missing

Check that PostgreSQL is healthy:

```bash
docker compose ps postgres
```

Inspect its logs:

```bash
docker compose logs --tail=100 postgres
```

Verify the SQL initialization file is mounted:

```bash
docker exec postgres ls -l /docker-entrypoint-initdb.d/
```

You can also trigger the Airflow DAG to execute `create_gold_tables` before publishing the metrics.

### Airflow tasks fail

Inspect the Airflow scheduler logs:

```bash
docker compose logs --tail=100 airflow-scheduler
```

Open the failed task in the Airflow interface and review its logs for the underlying error.

### dbt tests fail

Inspect the logs of the `run_dbt` and `test_dbt` tasks in Airflow.

Verify that the PostgreSQL connection settings in `.env` match those used by the dbt project.



## Project progress

- [x] Setup infrastructure (Docker, Kafka, Spark, Postgres)
- [x] Kafka Ingestion 
    <details>
    <summary>Click to see details</summary>

    - Kafka topic creation: 
    ```docker exec kafka kafka-topics --create --topic ecommerce_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 ```
    - List topics: ```docker exec kafka kafka-topics --list --bootstrap-server localhost:9092```
    - Run main.py (Start docker ingestion container)
    - Check a topic content: ```docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce_events --from-beginning```
- [x] Spark Streaming
    <details>
    <summary>Click to see details</summary>

    - [x] parsing JSON + typage + Kafka metadata
    - [x] Bronze Layer
        <details>
        <summary>Click to see details</summary>

        - Run bronze stream spark-submit: ```docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/bronze_stream.py```
    - [x] Silver Layer
        <details>
        <summary>Click to see details</summary>
        
        - Run silver stream spark-submit: ```docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/silver_stream.py```
    - [x] Gold Layer
        <details>
        <summary>Click to see details</summary>

        - Run hourly metrics stream spark-submit: ```docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/hourly_metrics.py```
        - Run product metrics stream spark-submit: ```docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/product_metrics.py```
        - Run user metrics stream spark-submit: ```docker exec spark-job /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/jobs/user_metrics.py```
    - [x] Pipeline transformation tests
        <details>
        <summary>Click to see details</summary>

        - test silver_stream : ```docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_silver.py```
        - test bronze_stream : ```docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_bronze.py```
        - test hourly_metrics: ```docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_hourly_metrics.py```
        - test product_metrics: ```docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_product_metrics.py```
        - test user_metrics   : ```docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_user_metrics.py```
        - test all from /tests/unit : ```docker exec -it spark-job pytest -v /opt/spark/tests/unit```
    - [x] Serving Layer
        <details>
        <summary>Click to see details</summary>

        - [x] create sql gold tables : -> bash: ```docker exec -i postgres psql -U data_user -d ecommerce < sql/serving/create_gold_tables.sql```
                                   -> PowerShell: ```Get-Content sql/serving/create_gold_tables.sql | docker exec -i postgres psql -U data_user -d ecommerce```
        - [x] publish hourly metrics into PostgreSQL. Run with ```docker exec -it spark-job python3 /opt/spark/jobs/serving/publish_hourly_metrics.py```
        - [x] publish product metrics into PostgreSQL. Run with ```docker exec -it spark-job python3 /opt/spark/jobs/serving/publish_product_metrics.py```
        - [x] publish user metrics into PostgreSQL. Run with ```docker exec -it spark-job python3 /opt/spark/jobs/serving/publish_user_metrics.py```
    - [x] Serving Layer Tests
        <details>
        <summary>Click to see details</summary>

        - [x] integration test JDBC connection : PowerShell: ```docker exec -it spark-job python3 /opt/spark/tests/integration/test_jdbc_connection.py```
        - [x] integration test jdbc upsert : ```docker exec -it spark-job python3 /opt/spark/tests/integration/test_jdbc_upsert.py```
        - [x] unit test postgres : ```docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_postgres.py``` 
        - [x] unit test hourly_metrics_serving : ```docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_hourly_metrics_serving.py```
        - [x] unit test product_metrics_serving : ```docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_product_metrics_serving.py```
        - [x] unit test user_metrics_serving : ```docker exec -it spark-job pytest -v /opt/spark/tests/unit/test_user_metrics_serving.py```
    - [x] Create Streaming containers
        <details>
        <summary>Click to see details</summary>

        - [x] Bronze stream
        - [x] Silver stream
        - [x] Gold stream (hourly & product & user)
- [x] Data modeling (dbt)
    <details>
    <summary>Click to see details</summary>

    - [x] dbt installation (docker container)
        - Check dbt version : ```docker exec -it dbt dbt --version```
    - [x] Define dbt project, profile & adapter (PostgreSQL)
        - Diagnoctic dbt configuration : ```docker exec -it dbt dbt debug```
    - [x] Define dbt sources
        - Check if dbt soucres are detected: ```docker exec -it dbt dbt ls```
    - Run models in folder/package : ```docker exec -it dbt dbt run --select folder_name```
    - Run model with its dependances : ```docker exec -it dbt dbt run --select "+model_name"```
    - Run dbt parsing : ```docker exec -it dbt dbt parse```
    - [x] Staging models
        <details>
        <summary>Click to see details</summary>

        - [x] Hourly model
        - [x] Product model
        - [x] user model
    - [x] Intermediate models
        <details>
        <summary>Click to see details</summary>

        - [x] Product model
        - [x] User model
        - [x] Hourly model
    - [x] Mart models
        <details>
        <summary>Click to see details</summary>

        - [x] Product model
        - [x] User model
        - [x] Hourly model
    - [x] tests
        <details>
        <summary>Click to see details</summary>

        - [x] Product model
        - [x] User model
        - [x] Hourly model
        - Run dbt tests : ```docker exec -it dbt dbt test --select model_name```
    - [x] Generate dbt documentation
        <details>
        <summary>Click to see details</summary>

        - [x] Product model
        - [X] User model
        - [x] Hourly model
        - Generate dbt documentation : ```docker exec -it dbt dbt docs generate```
        - Run dbt docs server : ```docker exec -it dbt dbt docs serve --host 0.0.0.0 --port 8080```
- [X] Orchestration (Airflow)
    <details>
    <summary>Click to see details</summary>

    - [x] Airflow installation (init, webserver & scheduler)
    - [x] DAG 0 : First Test as Validation DAG
    - [x] DAG 1 : ecommerce pipeline (Gold metrics -> PostgreSQL Serving -> dbt transformations)
        <details>
        <summary>Click to see details</summary>

        - [x] Gold files check task
        - [x] Postgres serving task : publish data into PostgreSQL
        - [x] dbt task (Run dbt transformations)
- [ ] BI dashboard
- [ ] Final documentation and repository cleanup


## Future Improvements

Potential extensions to the project include:

* Improve documentation with architecture diagrams and execution screenshots.
* Stronger data freshness checks for Gold datasets.
* Improved handling of late-arriving events.
* More comprehensive data quality checks.
* Monitoring and alerting for streaming applications.
* Add monitoring and alerting for pipeline failures.
* CI/CD automation for code quality and pipeline validation.
* Cloud deployment and integration with a cloud data warehouse.

## Author

**Yacouba Abass KABORE**

Data Engineer focused on data pipelines, streaming architectures, and observability.

* GitHub: [@absonkab](https://github.com/absonkab)

---

This project is built for learning and portfolio purposes. It demonstrates the integration of streaming ingestion, distributed processing, analytical data modeling, orchestration, and automated data quality validation.