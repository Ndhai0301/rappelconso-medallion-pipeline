import logging
from pathlib import Path

import psycopg2

from src.constants import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_PROPERTIES,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_FILES = (
    PROJECT_ROOT / "sql" / "init.sql",
    PROJECT_ROOT / "sql" / "migrate_add_dedup_key.sql",
)


def connect():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB,
        user=POSTGRES_PROPERTIES["user"],
        password=POSTGRES_PROPERTIES["password"],
    )


def run_migrations() -> None:
    with connect() as connection:
        with connection.cursor() as cursor:
            for migration_file in MIGRATION_FILES:
                cursor.execute(migration_file.read_text(encoding="utf-8"))
                logging.info("Applied PostgreSQL migration %s", migration_file.name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_migrations()
