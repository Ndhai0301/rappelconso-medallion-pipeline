from contextlib import contextmanager

from src.postgres_client import migrate


def test_run_migrations_executes_all_sql_files(tmp_path, monkeypatch):
    first = tmp_path / "001.sql"
    second = tmp_path / "002.sql"
    first.write_text("SELECT 1;", encoding="utf-8")
    second.write_text("SELECT 2;", encoding="utf-8")
    executed = []

    class Cursor:
        def execute(self, sql):
            executed.append(sql)

    @contextmanager
    def cursor_context():
        yield Cursor()

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return cursor_context()

    monkeypatch.setattr(migrate, "MIGRATION_FILES", (first, second))
    monkeypatch.setattr(migrate, "connect", lambda: Connection())

    migrate.run_migrations()

    assert executed == ["SELECT 1;", "SELECT 2;"]
