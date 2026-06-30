import datetime as dt
import json

from src.kafka_client import kafka_stream_data as producer


def test_parse_publication_datetime():
    parsed = producer.parse_publication_datetime("2026-06-29T12:30:00Z")
    assert parsed == dt.datetime(2026, 6, 29, 12, 30, tzinfo=dt.timezone.utc)
    assert producer.parse_publication_datetime("not-a-date") is None


def test_deduplicate_keeps_latest_occurrence():
    records = [
        {"id": 1, "rappel_guid": "same", "numero_version": 1},
        {"id": 1, "rappel_guid": "same", "numero_version": 2},
        {"id": 2, "rappel_guid": "other", "numero_version": 1},
    ]
    result = producer.deduplicate_data(records)
    assert len(result) == 2
    assert result[0]["numero_version"] == 2


def test_cursor_is_written_atomically(tmp_path, monkeypatch):
    cursor_path = tmp_path / "last_processed.json"
    monkeypatch.setattr(producer, "LAST_PROCESSED_PATH", cursor_path)
    monkeypatch.setattr(producer, "CURSOR_OVERLAP_DAYS", 1)

    producer.update_last_processed_file(
        [{"date_publication": "2026-06-29T12:30:00+00:00"}]
    )

    assert json.loads(cursor_path.read_text()) == {"last_processed": "2026-06-28"}
    assert not cursor_path.with_suffix(".tmp").exists()


def test_process_data_adds_utc_ingestion_timestamp():
    result = producer.process_data({"id": 10})
    assert result["id"] == 10
    assert result["raw_ingested_at_utc"].endswith("+00:00")


def test_get_all_data_paginates(monkeypatch):
    pages = {
        0: [
            {"id": index, "date_publication": "2026-01-01T00:00:00+00:00"}
            for index in range(100)
        ],
        100: [{"id": 101, "date_publication": "2026-01-02T00:00:00+00:00"}],
    }
    monkeypatch.setattr(producer, "MAX_OFFSET", 1000)
    monkeypatch.setattr(
        producer,
        "fetch_page",
        lambda _timestamp, offset: pages.get(offset, []),
    )
    assert len(producer.get_all_data("2025-12-31")) == 101
