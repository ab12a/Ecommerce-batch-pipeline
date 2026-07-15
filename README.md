# Ecommerce Batch Data Pipeline

![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Spark](https://img.shields.io/badge/Apache-Spark-orange)
![Kafka](https://img.shields.io/badge/Apache-Kafka-black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Python](https://img.shields.io/badge/Python-3.8+-green)

An end-to-end batch data processing pipeline built with **Apache Spark**, **Apache Kafka**, **PostgreSQL**, **Apache Airflow**, **MinIO**, and **Docker Compose**.

The project demonstrates how modern data engineering technologies can be integrated into a reproducible microservices architecture for ingesting, processing, storing, and orchestrating e-commerce clickstream data.

---

# Table of Contents

- Project Overview
- Architecture
- Technologies Used
- Project Structure
- Data Pipeline Workflow
- Prerequisites
- Installation
- Environment Configuration
- Custom Spark Image
- Dataset
- Running the Pipeline
- SQL Queries
- Results
- Phase 3 Improvements
- Future Improvements
- Author

---

# Project Overview

The objective of this project is to build a reproducible batch data engineering pipeline capable of processing large-scale e-commerce clickstream data.

The implemented pipeline performs the following tasks:

- Reads raw e-commerce clickstream data
- Cleans and preprocesses the dataset using Apache Spark
- Stores the processed dataset in Parquet format
- Publishes processed records to Apache Kafka
- Consumes Kafka events
- Exports Kafka events to JSON
- Persists processed records in PostgreSQL
- Performs analytical aggregations using Spark
- Supports workflow orchestration with Apache Airflow

The complete environment is containerized using Docker Compose, allowing the project to be deployed consistently on another machine.

---

# Architecture

```text
                    +----------------------+
                    |  Ecommerce Dataset   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Apache Spark ETL   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Parquet Dataset    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Kafka Producer     |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |     Kafka Topic      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Kafka Consumer     |
                    +----------+-----------+
                               |
              +----------------+----------------+
              |                                 |
              v                                 v
     +----------------------+          +----------------------+
     |   JSON Output File   |          |     PostgreSQL       |
     +----------------------+          +----------------------+
                                                  ^
                                                  |
                                        +----------------------+
                                        |   Apache Airflow     |
                                        +----------------------+
```

---

# Technologies Used

| Technology | Purpose |
|------------|---------|
| Apache Spark | Batch ETL processing |
| Apache Kafka | Event streaming |
| PostgreSQL | Persistent storage |
| Apache Airflow | Workflow orchestration |
| Docker | Containerization |
| Docker Compose | Multi-container orchestration |
| MinIO | Object storage |
| Python | ETL scripting |

---

# Project Structure

```text
ecommerce_batch_pipeline/

├── airflow/
│   ├── dags/
│   ├── logs/
│   └── plugins/
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── kafka_output/
│
├── output_screenshots/
│
├── spark/
│   └── scripts/
│       ├── read_data.py
│       ├── read_parquet.py
│       ├── kafka_producer.py
│       ├── kafka_consumer.py
│       ├── kafka_2_postgres.py
│       ├── aggregate_data.py
│       └── sql/
│           └── sql_sample_test.sql
│
├── Dockerfile
├── docker-compose.yml
├── .env
├── README.md
└── .gitignore
```

---

# Data Pipeline Workflow

1. Load the raw e-commerce dataset.
2. Apache Spark cleans and preprocesses the data.
3. Store the cleaned data in Parquet format.
4. Kafka Producer publishes processed records.
5. Kafka Consumer reads the events.
6. Events are exported to JSON.
7. Events are stored in PostgreSQL.
8. Spark performs batch aggregations.
9. Apache Airflow can orchestrate the workflow.

---

# Prerequisites

Install the following software before running the project:

- Docker Desktop
- Git
- Visual Studio Code (recommended)

---

# Installation

Clone the repository:

```bash
git clone https://github.com/ab12a/Ecommerce-batch-pipeline.git
```

Navigate to the project directory:

```bash
cd Ecommerce-batch-pipeline
```

Build the custom Spark image:

```bash
docker compose build
```

Start all services:

```bash
docker compose up -d
```

Verify the running containers:

```bash
docker ps
```

---

# Environment Configuration

The project uses a centralized `.env` file to manage configuration values.

The following parameters can be configured:

- Spark master URL
- Kafka broker
- Kafka topic
- PostgreSQL host, database, user, and password
- MinIO credentials
- Dataset paths
- Output locations

Docker Compose automatically loads these variables when the services start.

> **Note:** The included `.env` file contains default credentials intended for local development and demonstration purposes only. Production deployments should use secure secrets management solutions.

---

# Custom Spark Image

A custom Apache Spark Docker image is used to provide a reproducible execution environment.

The image automatically installs the required Python packages, including:

- kafka-python
- psycopg2-binary

This removes the need for manual dependency installation and improves portability across different environments.

---

# Dataset

This project uses the **October 2019 E-commerce Behavior Dataset**.

Dataset:

https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

The dataset is not included in this repository because of its size.

Place the following file inside:

```text
data/raw/
```

```
2019-Oct.csv.gz
```

For faster execution during development and testing, the ETL job processes the first **10,000 records**. This limit can be modified or removed in `read_data.py`.

---

# Running the Pipeline

Build the custom Spark image (first run only):

```bash
docker compose build
```

Start the environment:

```bash
docker compose up -d
```

Run the Spark ETL job:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/read_data.py
```

Verify the generated Parquet dataset:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/read_parquet.py
```

Publish records to Kafka:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/kafka_producer.py
```

Consume Kafka events and export them to JSON:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/kafka_consumer.py
```

Create the `customer_events` table (first run only) using the SQL script in:

```text
spark/scripts/sql/sql_sample_test.sql
```

Then load Kafka events into PostgreSQL:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/kafka_2_postgres.py
```

Run the aggregation job:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/aggregate_data.py
```

Verify the imported data:

```bash
docker exec -it postgres psql -U admin -d ecommerce
```

Example:

```sql
SELECT COUNT(*) FROM customer_events;
```

Stop the environment:

```bash
docker compose down
```

---

# SQL Queries

Example SQL queries are available in:

```text
spark/scripts/sql/sql_sample_test.sql
```

The queries demonstrate how to:

- Validate imported data
- Count records
- Analyze event types
- Analyze product brands
- Analyze user activity
- Analyze pricing information

---

# Results

The pipeline was successfully tested end-to-end.

The completed workflow demonstrates:

- CSV ingestion and preprocessing
- Spark ETL processing
- Parquet dataset generation
- Kafka message publishing
- Kafka message consumption
- JSON export
- PostgreSQL persistence
- Spark batch aggregation
- SQL validation
- Airflow workflow orchestration

---

# Phase 3 Improvements

The final phase introduced several improvements:

- Custom Apache Spark Docker image
- Automatic installation of Python dependencies
- Centralized configuration using `.env`
- Improved logging
- Exception handling
- SQL validation queries
- Improved Docker Compose configuration
- End-to-end pipeline testing
- Cleaner project organization
- Improved documentation

---

# Future Improvements

Possible future enhancements include:

- Spark Structured Streaming for real-time processing
- CI/CD using GitHub Actions
- Centralized logging and monitoring
- Secure secrets management
- Cloud deployment (AWS, Azure, or GCP)
- Dashboard visualization using Grafana

---

# Author

**Abhyuday Mahanta**

IU International University of Applied Sciences

Data Engineering Project
