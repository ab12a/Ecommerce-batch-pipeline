# Ecommerce Batch Data Pipeline

An end-to-end batch data processing pipeline built with **Apache Spark**, **Apache Kafka**, **PostgreSQL**, **Apache Airflow**, **MinIO**, and **Docker Compose**.

This project demonstrates how modern data engineering technologies can be integrated into a reproducible microservices architecture for ingesting, processing, storing, and orchestrating e-commerce event data.

---

# Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Data Pipeline Workflow](#data-pipeline-workflow)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Environment Configuration](#environment-configuration)
- [Custom Spark Image](#custom-spark-image)
- [Dataset](#dataset)
- [Running the Pipeline](#running-the-pipeline)
- [SQL Queries](#sql-queries)
- [Results](#results)
- [Phase 3 Improvements](#phase-3-improvements)
- [Future Improvements](#future-improvements)
- [Author](#author)

---

# Project Overview

The objective of this project is to build a reproducible batch data processing system capable of processing large-scale e-commerce event data.

The pipeline performs the following tasks:

- Ingests raw e-commerce event data
- Cleans and preprocesses the dataset using Apache Spark
- Stores cleaned data in Parquet format
- Streams processed events through Apache Kafka
- Stores processed records in PostgreSQL
- Performs batch aggregations using Spark
- Orchestrates workflow execution using Apache Airflow

The complete environment is deployed locally using Docker Compose.

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
| Docker Compose | Container orchestration |
| Docker | Containerization |
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
├── spark/
│   └── scripts/
│       ├── read_data.py
│       ├── read_parquet.py
│       ├── kafka_producer.py
│       ├── kafka_consumer.py
│       ├── kafka_2_postgres.py
│       └── aggregate_data.py
│
├── sql/
│   └── sample_queries.sql
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
3. Cleaned data is stored in Parquet format.
4. Kafka Producer publishes events to the `customer-events` topic.
5. Kafka Consumer reads events from Kafka.
6. Events are written to a JSON file.
7. Events are stored in PostgreSQL.
8. Spark performs analytical aggregations.
9. Apache Airflow orchestrates the workflow.

---

# Prerequisites

Before running the project, install:

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

Verify running containers:

```bash
docker ps
```

---

# Environment Configuration

The project uses a `.env` file to centralize configuration values.

The following settings are configurable:

- Spark master URL
- Raw dataset path
- Processed Parquet path
- Kafka bootstrap server
- Kafka topic
- Kafka output path
- PostgreSQL credentials
- MinIO credentials

Docker Compose automatically loads these variables during startup.

---

# Custom Spark Image

A custom Docker image is used for Apache Spark.

The image automatically installs the required Python packages:

- kafka-python
- psycopg2-binary

This eliminates manual package installation and improves reproducibility.

---

# Dataset

This project uses the **October 2019 E-commerce Dataset**.

> **Note:** The dataset is not included in this repository because of its large size.

Download the dataset separately and place it in:

```text
data/raw/
```

---

# Running the Pipeline

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

Consume Kafka messages and write them to JSON:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/kafka_consumer.py
```

Consume Kafka messages and store them in PostgreSQL:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/kafka_2_postgres.py
```

Run Spark aggregation:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/aggregate_data.py
```

Verify PostgreSQL:

```bash
docker exec -it postgres psql -U admin -d ecommerce
```

Example query:

```sql
SELECT COUNT(*) FROM customer_events;
```

---

# SQL Queries

Example SQL queries are provided in:

```text
sql/sample_queries.sql
```

These queries demonstrate how to:

- Validate imported data
- Count records
- Analyse event types
- Analyse product brands
- Analyse user activity
- Explore product pricing

---

# Results

The pipeline was successfully tested end-to-end.

The completed workflow performs:

- CSV ingestion
- Spark ETL processing
- Parquet generation
- Kafka message publishing
- Kafka message consumption
- JSON export
- PostgreSQL data persistence
- Spark aggregation
- SQL validation queries

---

# Phase 3 Improvements

The project was enhanced with several improvements:

- Custom Spark Docker image
- Automatic installation of Python dependencies
- Centralized configuration using `.env`
- Improved logging
- Exception handling
- SQL validation queries
- Improved Docker Compose configuration
- End-to-end pipeline testing
- Cleaner and more maintainable project structure

---

# Future Improvements

Possible future enhancements include:

- Spark Structured Streaming for real-time processing
- CI/CD pipeline using GitHub Actions
- Centralized logging and monitoring
- Cloud deployment (AWS, Azure, or GCP)
- Secrets management for credentials
- Dashboard visualization using Grafana

---

# Author

**Abhyuday Mahanta**

IU International University of Applied Sciences

Data Engineering Project
