from contextlib import contextmanager

from src.postgres_client import audit


class FakeCursor:
    def __init__(self, responses):
        self._responses = list(responses)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._responses.pop(0)


@contextmanager
def cursor_context(cursor):
    yield cursor


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return cursor_context(self._cursor)


def test_get_row_counts_returns_none_for_missing_gold_table():
    cursor = FakeCursor([(10,), (5,), (None,)])
    connection = FakeConnection(cursor)

    counts = audit.get_row_counts(connection)

    assert counts == {"bronze": 10, "silver": 5, "gold": None}


def test_get_row_counts_includes_gold_when_table_exists():
    cursor = FakeCursor([(10,), (5,), ("gold.fact_rappel",), (3,)])
    connection = FakeConnection(cursor)

    counts = audit.get_row_counts(connection)

    assert counts == {"bronze": 10, "silver": 5, "gold": 3}


class FakeTaskInstance:
    def __init__(self, task_id, state):
        self.task_id = task_id
        self.state = state


class FakeDagRun:
    def __init__(self, states):
        self._instances = [
            FakeTaskInstance(task_id, state) for task_id, state in states.items()
        ]

    def get_task_instances(self):
        return self._instances


def test_determine_run_status_success_when_all_siblings_succeed():
    dag_run = FakeDagRun({"migrate_postgres": "success", "log_audit": "running"})
    assert audit.determine_run_status(dag_run, "log_audit") == "success"


def test_determine_run_status_failed_when_any_sibling_failed():
    dag_run = FakeDagRun({"migrate_postgres": "failed", "log_audit": "running"})
    assert audit.determine_run_status(dag_run, "log_audit") == "failed"


def test_determine_run_status_partial_otherwise():
    dag_run = FakeDagRun({"migrate_postgres": "skipped", "log_audit": "running"})
    assert audit.determine_run_status(dag_run, "log_audit") == "partial"
