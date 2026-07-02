from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from src.postgres_client.audit import log_audit_run


PROJECT_DIR = "/opt/airflow/project"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt/rappelconso"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"

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

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"{DBT_BIN} deps --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"{DBT_BIN} snapshot --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{DBT_BIN} run --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_BIN} test --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    log_audit = PythonOperator(
        task_id="log_audit",
        python_callable=log_audit_run,
        trigger_rule="all_done",
    )

    migrate_postgres >> fetch_and_produce >> spark_to_postgres >> dbt_deps >> dbt_snapshot >> dbt_run >> dbt_test >> log_audit
