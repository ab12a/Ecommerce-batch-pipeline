from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from Airflow!")

with DAG(
    dag_id="test_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )