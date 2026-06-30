from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="rappelconso_pipeline",
    description="RappelConso: API to Kafka to Spark to PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["rappelconso", "france", "data-engineering"],
) as dag:
    migrate_postgres = BashOperator(
        task_id="migrate_postgres",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m src.postgres_client.migrate"
        ),
    )

    fetch_and_produce = BashOperator(
        task_id="fetch_and_produce",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m src.kafka_client.kafka_stream_data"
        ),
    )

    spark_to_postgres = BashOperator(
        task_id="spark_to_postgres",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m src.spark_client.spark_stream_data"
        ),
    )

    migrate_postgres >> fetch_and_produce >> spark_to_postgres
