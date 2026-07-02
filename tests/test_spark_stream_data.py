import datetime as dt

from src.spark_client.spark_stream_data import (
    DEDUPLICATION_KEY,
    add_deduplication_key,
    build_upsert_sql,
    select_latest_versions,
    write_bronze,
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
    sql = build_upsert_sql("silver.test_staging")
    assert "INSERT INTO silver.rappelconso_clean" in sql
    assert "raw_data::jsonb" in sql
    assert "identification_produits::jsonb" in sql
    assert "ON CONFLICT (deduplication_key)" in sql
    assert "silver_updated_at = now()" in sql
    # Strictly newer versions always win...
    assert "EXCLUDED.numero_version > silver.rappelconso_clean.numero_version" in sql
    # ...but a same-version re-upsert must only touch the row (and bump
    # silver_updated_at, which drives the dbt snapshot) when content actually
    # changed, otherwise replays/reruns fabricate fake SCD2 history.
    assert "EXCLUDED.numero_version = silver.rappelconso_clean.numero_version" in sql
    assert "IS DISTINCT FROM EXCLUDED." in sql
    assert "numero_version IS DISTINCT FROM" not in sql
    # raw_data embeds a fresh raw_ingested_at_utc on every re-fetch (cursor
    # overlap replays), so it must not participate in the content diff or
    # every harmless replay would fabricate a new SCD2 version.
    assert "raw_data IS DISTINCT FROM" not in sql


def test_write_bronze_captures_every_message_including_malformed(spark, monkeypatch):
    dataframe = spark.createDataFrame(
        [
            (
                '{"id": 1, "rappel_guid": "guid-1", "numero_fiche": "sheet-1", '
                '"numero_version": 2, "date_publication": "2026-01-02T00:00:00"}',
            ),
            ("not-valid-json{{{",),
            (None,),
        ],
        ["value"],
    )

    captured = {}

    class FakeWriter:
        def __init__(self, df):
            self._df = df

        def jdbc(self, url, table, mode, properties):
            captured["table"] = table
            captured["mode"] = mode
            captured["rows"] = self._df.collect()

    monkeypatch.setattr(type(dataframe), "write", property(lambda self: FakeWriter(self)))

    write_bronze(dataframe, batch_id=0)

    assert captured["table"] == "bronze.rappelconso_events"
    assert captured["mode"] == "append"

    rows = captured["rows"]
    # Every input message produces exactly one bronze row, including
    # malformed JSON and a null Kafka value -- bronze never drops a message.
    assert len(rows) == 3

    valid_row = next(r for r in rows if r[DEDUPLICATION_KEY] == "guid-1")
    assert valid_row.numero_version == 2
    assert "guid-1" in valid_row.raw_json

    malformed_row = next(r for r in rows if r.raw_json == "not-valid-json{{{")
    assert malformed_row[DEDUPLICATION_KEY] is None
    assert malformed_row.api_id is None

    null_value_row = next(r for r in rows if r.raw_json == "{}")
    assert null_value_row[DEDUPLICATION_KEY] is None
