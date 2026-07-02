import ast
from pathlib import Path


def test_dag_contains_pipeline_tasks_in_source():
    dag_path = Path("dags/rappelconso_pipeline.py")
    ast.parse(dag_path.read_text(encoding="utf-8"))
    source = dag_path.read_text(encoding="utf-8")
    assert 'task_id="migrate_postgres"' in source
    assert 'task_id="fetch_and_produce"' in source
    assert 'task_id="spark_to_postgres"' in source
    assert 'task_id="dbt_deps"' in source
    assert 'task_id="dbt_snapshot"' in source
    assert 'task_id="dbt_run"' in source
    assert 'task_id="dbt_test"' in source
    assert 'task_id="dbt_docs_generate"' in source
    assert 'task_id="log_audit"' in source
    assert (
        "migrate_postgres >> fetch_and_produce >> spark_to_postgres >> "
        "dbt_deps >> dbt_snapshot >> dbt_run >> dbt_test >> "
        "dbt_docs_generate >> log_audit"
    ) in source
