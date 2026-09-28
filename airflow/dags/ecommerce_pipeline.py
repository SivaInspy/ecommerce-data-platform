import os

from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


SPARK_IMAGE = "ecommerce-spark:latest"


SPARK_PROJECT_PATH = os.environ.get(
    "SPARK_PROJECT_PATH",
    "C:/Users/dsiva/OneDrive/Projects/ecommerce-data-platform/spark"
)


spark_mounts = [

    Mount(
        source=f"{SPARK_PROJECT_PATH}/bronze",
        target="/opt/spark-app/bronze",
        type="bind",
    ),

    Mount(
        source=f"{SPARK_PROJECT_PATH}/silver",
        target="/opt/spark-app/silver",
        type="bind",
    ),

    Mount(
        source=f"{SPARK_PROJECT_PATH}/gold",
        target="/opt/spark-app/gold",
        type="bind",
    ),

    Mount(
        source=f"{SPARK_PROJECT_PATH}/quarantine",
        target="/opt/spark-app/quarantine",
        type="bind",
    ),

    Mount(
        source=f"{SPARK_PROJECT_PATH}/checkpoint",
        target="/opt/spark-app/checkpoint",
        type="bind",
    ),

    Mount(
        source=f"{SPARK_PROJECT_PATH}/checkpoint_silver",
        target="/opt/spark-app/checkpoint_silver",
        type="bind",
    ),
]


with DAG(
    dag_id="ecommerce_data_pipeline",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=[
        "ecommerce",
        "data-engineering",
        "spark",
        "docker",
    ],
) as dag:


    silver_processing = DockerOperator(

        task_id="silver_processing",

        image=SPARK_IMAGE,

        command=[
            "python3",
            "run_silver_batch.py",
        ],

        working_dir="/opt/spark-app",

        docker_url="unix:///var/run/docker.sock",

        mounts=spark_mounts,

        auto_remove="success",

        mount_tmp_dir=False,
    )


    gold_processing = DockerOperator(

        task_id="gold_processing",

        image=SPARK_IMAGE,

        command=[
            "python3",
            "run_gold.py",
        ],

        working_dir="/opt/spark-app",

        docker_url="unix:///var/run/docker.sock",

        mounts=spark_mounts,

        auto_remove="success",

        mount_tmp_dir=False,
    )


    gold_validation = DockerOperator(

        task_id="gold_validation",

        image=SPARK_IMAGE,

        command=[
            "python3",
            "validate_gold_metrics.py",
        ],

        working_dir="/opt/spark-app",

        docker_url="unix:///var/run/docker.sock",

        mounts=spark_mounts,

        auto_remove="success",

        mount_tmp_dir=False,
    )


    silver_processing >> gold_processing >> gold_validation