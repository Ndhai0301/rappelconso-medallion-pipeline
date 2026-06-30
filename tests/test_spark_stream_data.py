import datetime as dt

from src.spark_client.spark_stream_data import (
    DEDUPLICATION_KEY,
    add_deduplication_key,
    build_upsert_sql,
    select_latest_versions,
)


def test_deduplication_key_fallbacks(spark):
    dataframe = spark.createDataFrame(
        [
            (1, "guid", "sheet"),
            (2, None, "sheet-2"),
            (3, None, None),
        ],
        ["api_id", "rappel_guid", "numero_fiche"],
    )
    keys = [row[DEDUPLICATION_KEY] for row in add_deduplication_key(dataframe).collect()]
    assert keys == ["guid", "sheet-2", "3"]


def test_latest_version_wins(spark):
    dataframe = spark.createDataFrame(
        [
            ("same", 1, dt.datetime(2026, 1, 1), 1),
            ("same", 3, dt.datetime(2026, 1, 2), 1),
            ("same", 2, dt.datetime(2026, 1, 3), 1),
        ],
        [DEDUPLICATION_KEY, "numero_version", "date_publication", "api_id"],
    )
    row = select_latest_versions(dataframe).collect()[0]
    assert row.numero_version == 3


def test_upsert_sql_casts_json_and_rejects_old_versions():
    sql = build_upsert_sql("dev.test_staging")
    assert "raw_data::jsonb" in sql
    assert "identification_produits::jsonb" in sql
    assert "ON CONFLICT (deduplication_key)" in sql
    assert "EXCLUDED.numero_version >=" in sql
