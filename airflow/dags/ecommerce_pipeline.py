from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def silver_task():
    print("Starting Silver processing...")


def gold_task():
    print("Starting Gold processing...")


def validation_task():
    print("Starting data quality validation...")


with DAG(
    dag_id="ecommerce_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "data-engineering"],
) as dag:

    silver = PythonOperator(
        task_id="silver_processing",
        python_callable=silver_task,
    )

    gold = PythonOperator(
        task_id="gold_processing",
        python_callable=gold_task,
    )

    validation = PythonOperator(
        task_id="gold_validation",
        python_callable=validation_task,
    )

    silver >> gold >> validation