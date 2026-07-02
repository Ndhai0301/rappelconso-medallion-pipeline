import json
import logging
from typing import Any

from src.postgres_client.migrate import connect


ROW_COUNT_QUERIES = {
    "bronze": "SELECT count(*) FROM bronze.rappelconso_events",
    "silver": "SELECT count(*) FROM silver.rappelconso_clean",
    "gold": "SELECT count(*) FROM gold.fact_rappel",
}


def get_row_counts(connection) -> dict[str, int | None]:
    counts: dict[str, int | None] = {}
    with connection.cursor() as cursor:
        for layer, query in ROW_COUNT_QUERIES.items():
            if layer == "gold":
                cursor.execute("SELECT to_regclass('gold.fact_rappel')")
                if cursor.fetchone()[0] is None:
                    counts[layer] = None
                    continue
            cursor.execute(query)
            counts[layer] = cursor.fetchone()[0]
    return counts


def determine_run_status(dag_run, current_task_id: str) -> str:
    task_instances = dag_run.get_task_instances()
    sibling_states = [
        ti.state for ti in task_instances if ti.task_id != current_task_id
    ]
    if any(state in ("failed", "upstream_failed") for state in sibling_states):
        return "failed"
    if all(state == "success" for state in sibling_states):
        return "success"
    return "partial"


def log_audit_run(**context: Any) -> None:
    dag_run = context["dag_run"]
    task_instance = context["task_instance"]
    started_at = dag_run.start_date
    status = determine_run_status(dag_run, task_instance.task_id)

    with connect() as connection:
        counts = get_row_counts(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO audit_pipeline_runs
                    (dag_run_id, started_at, ended_at, duration_seconds, status,
                     bronze_row_count, silver_row_count, gold_row_count, details)
                VALUES
                    (%s, %s, now(), EXTRACT(EPOCH FROM (now() - %s)), %s, %s, %s, %s, %s)
                """,
                (
                    dag_run.run_id,
                    started_at,
                    started_at,
                    status,
                    counts["bronze"],
                    counts["silver"],
                    counts["gold"],
                    json.dumps(counts),
                ),
            )

    logging.info("Audit logged for run %s: status=%s counts=%s", dag_run.run_id, status, counts)
