# Ecommerce Batch Pipeline

## Overview
This project implements an end-to-end batch data processing pipeline for an e-commerce dataset using a Dockerized microservices architecture.

## Technologies
- Apache Spark
- Apache Kafka
- Apache Airflow
- PostgreSQL
- MinIO
- Docker & Docker Compose
- Python

## Pipeline
1. Raw e-commerce dataset is ingested.
2. Spark cleans and preprocesses the data.
3. Processed events are exported in JSON format.
4. Kafka streams the events.
5. Kafka Consumer stores the data in PostgreSQL.
6. Spark performs batch aggregations.
7. Airflow orchestrates the workflow.

## Project Structure

```
airflow/
spark/
data/
docker-compose.yml
README.md
```

## How to Run

```bash
docker compose up -d
```

## Dataset

October 2019 E-commerce Dataset.

## Author

Abhyuday Mahanta
