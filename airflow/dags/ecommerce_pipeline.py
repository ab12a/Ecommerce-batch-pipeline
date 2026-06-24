from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ecommerce"]
) as dag:

    start = EmptyOperator(task_id="start")
    etl = EmptyOperator(task_id="spark_etl")
    producer = EmptyOperator(task_id="kafka_producer")
    consumer = EmptyOperator(task_id="kafka_to_postgres")
    end = EmptyOperator(task_id="end")

    start >> etl >> producer >> consumer >> end