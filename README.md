# Ecommerce Batch Data Pipeline

An end to end batch data processing pipeline built with **Apache Spark**, **Apache Kafka**, **PostgreSQL**, **Apache Airflow**, and **Docker Compose**.

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
- [Running the Pipeline](#running-the-pipeline)
- [Results](#results)
- [Future Improvements](#future-improvements)
- [Author](#author)

---

# Project Overview

The objective of this project is to build a reproducible batch data processing system capable of processing large scale e-commerce event data.

The pipeline performs the following tasks:

- Ingests raw e-commerce event data
- Cleans and preprocesses the dataset using Apache Spark
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
                               v
                    +----------------------+
                    |     PostgreSQL       |
                    +----------+-----------+
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
| MinIO | Object storage |
| Python | ETL scripts |

---

# Project Structure

```text
ecommerce_batch_pipeline/

├── airflow/
│   └── dags/
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── kafka_output/
│
├── spark/
│   └── scripts/
│
├── docker-compose.yml
├── README.md
└── .gitignore
```

---

# Data Pipeline Workflow

1. Load the raw e-commerce dataset.
2. Apache Spark cleans and preprocesses the data.
3. Spark exports the processed records.
4. Kafka Producer publishes events to the `customer-events` topic.
5. Kafka Consumer reads events from Kafka.
6. Events are stored in PostgreSQL.
7. Spark performs analytical aggregations.
8. Apache Airflow orchestrates the overall workflow.

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

Start all services:

```bash
docker compose up -d
```

Verify running containers:

```bash
docker ps
```

---

# Dataset

This project uses the **October 2019 E-commerce Dataset**.

> **Note:** The dataset is not included in this repository because of its large size. Download it separately and place it in:

```text
data/raw/
```

---

# Running the Pipeline

Execute the Spark ETL job:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/scripts/read_data.py
```

Run the Kafka producer:

```bash
docker exec -it spark python3 /opt/spark/scripts/kafka_producer.py
```

Run the Kafka consumer:

```bash
docker exec -it spark python3 /opt/spark/scripts/kafka_2_postgres.py
```

Verify the data in PostgreSQL:

```sql
SELECT COUNT(*) FROM customer_events;
```

---

# Results

The completed pipeline successfully demonstrates:

- Dockerized microservices
- Spark ETL processing
- Kafka event streaming
- PostgreSQL persistence
- Batch aggregation using Spark
- Workflow orchestration using Airflow

---

# Future Improvements

Possible future enhancements include:

- Spark Structured Streaming for real-time processing
- CI/CD pipeline using GitHub Actions
- Centralized logging and monitoring
- Cloud deployment (AWS/Azure/GCP)
- Secrets management for credentials
- Dashboard visualization using Grafana

---

# Author

**Abhyuday Mahanta**

IU International University of Applied Sciences

Data Engineering Project
