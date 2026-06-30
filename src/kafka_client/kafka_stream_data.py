import datetime as dt
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.constants import MAX_LIMIT, MAX_OFFSET, PATH_LAST_PROCESSED, URL_API


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,
)
logging.getLogger("kafka").setLevel(logging.WARNING)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LAST_PROCESSED_PATH = PROJECT_ROOT / PATH_LAST_PROCESSED

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rappelconso_raw")
LOCAL_KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "LOCAL_KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"
)
DOCKER_KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "DOCKER_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
PUBLICATION_DATE_FIELD = "date_publication"
DEFAULT_LAST_PROCESSED = "1900-01-01"
MAX_INGEST_PAGES = int(os.getenv("MAX_INGEST_PAGES", "0"))
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "30"))
CURSOR_OVERLAP_DAYS = int(os.getenv("CURSOR_OVERLAP_DAYS", "1"))


def create_http_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP_SESSION = create_http_session()


def import_kafka_client_classes():
    original_path = list(sys.path)
    sys.path = [path for path in sys.path if Path(path or ".").resolve() != PROJECT_ROOT]
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaConnectionError, KafkaError, KafkaTimeoutError
        from kafka.serializer import Serializer
    finally:
        sys.path = original_path
    return KafkaProducer, (KafkaConnectionError, KafkaTimeoutError, KafkaError), Serializer


def create_json_serializer(serializer_base):
    class JsonBytesSerializer(serializer_base):
        def serialize(self, topic, headers, data):
            return json.dumps(data, ensure_ascii=False).encode("utf-8")

    return JsonBytesSerializer()


def parse_publication_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None

    normalized_value = value.replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(normalized_value)
    except ValueError:
        try:
            return dt.datetime.strptime(value[:10], "%Y-%m-%d")
        except ValueError:
            logging.warning("Invalid %s value: %s", PUBLICATION_DATE_FIELD, value)
            return None


def to_api_date(value: dt.datetime) -> str:
    return value.date().isoformat()


def _write_cursor(value: str) -> None:
    LAST_PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = LAST_PROCESSED_PATH.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps({"last_processed": value}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_path.replace(LAST_PROCESSED_PATH)


def get_latest_timestamp() -> str:
    if not LAST_PROCESSED_PATH.exists():
        logging.warning(
            "%s does not exist. Creating it with default date %s.",
            LAST_PROCESSED_PATH,
            DEFAULT_LAST_PROCESSED,
        )
        _write_cursor(DEFAULT_LAST_PROCESSED)
        return DEFAULT_LAST_PROCESSED

    try:
        data = json.loads(LAST_PROCESSED_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        logging.warning(
            "%s is unreadable or invalid JSON. Using default date %s.",
            LAST_PROCESSED_PATH,
            DEFAULT_LAST_PROCESSED,
        )
        return DEFAULT_LAST_PROCESSED

    return data.get("last_processed", DEFAULT_LAST_PROCESSED)


def update_last_processed_file(records: list[dict[str, Any]]) -> None:
    publication_dates = [
        parsed_date
        for parsed_date in (
            parse_publication_datetime(record.get(PUBLICATION_DATE_FIELD))
            for record in records
        )
        if parsed_date is not None
    ]

    if not publication_dates:
        logging.warning("No valid publication dates found. Cursor was not updated.")
        return

    next_cursor = max(publication_dates) - dt.timedelta(days=CURSOR_OVERLAP_DAYS)
    next_cursor_string = to_api_date(next_cursor)
    _write_cursor(next_cursor_string)
    logging.info("Updated last_processed to %s", next_cursor_string)


def fetch_page(last_processed_timestamp: str, offset: int) -> list[dict[str, Any]]:
    url = URL_API.format(last_processed_timestamp, offset)
    logging.info("Fetching API page offset=%s from %s", offset, last_processed_timestamp)

    response = HTTP_SESSION.get(url, timeout=API_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results", [])
    if not isinstance(results, list):
        raise ValueError("RappelConso API response has no list-valued 'results' field")
    return results


def get_latest_publication_date(records: list[dict[str, Any]]) -> dt.datetime | None:
    publication_dates = [
        parsed_date
        for parsed_date in (
            parse_publication_datetime(record.get(PUBLICATION_DATE_FIELD))
            for record in records
        )
        if parsed_date is not None
    ]
    return max(publication_dates) if publication_dates else None


def get_all_data(last_processed_timestamp: str) -> list[dict[str, Any]]:
    offset = 0
    page_count = 0
    full_data: list[dict[str, Any]] = []

    while True:
        current_results = fetch_page(last_processed_timestamp, offset)
        page_count += 1

        if not current_results:
            break

        full_data.extend(current_results)
        logging.info("Fetched %s records so far.", len(full_data))

        if len(current_results) < MAX_LIMIT:
            break

        offset += len(current_results)
        if MAX_INGEST_PAGES and page_count >= MAX_INGEST_PAGES:
            logging.info("Stopping after MAX_INGEST_PAGES=%s.", MAX_INGEST_PAGES)
            break

        if offset >= MAX_OFFSET:
            latest_publication_date = get_latest_publication_date(current_results)
            if latest_publication_date is None:
                raise RuntimeError("Cannot advance API cursor: page has no publication date")

            next_timestamp = to_api_date(
                latest_publication_date - dt.timedelta(days=CURSOR_OVERLAP_DAYS)
            )
            if next_timestamp <= last_processed_timestamp:
                raise RuntimeError(
                    "Cannot paginate safely because the publication-date cursor did not advance"
                )

            logging.info(
                "API offset limit reached. Moving cursor from %s to %s.",
                last_processed_timestamp,
                next_timestamp,
            )
            last_processed_timestamp = next_timestamp
            offset = 0

    logging.info("Got %s total records before deduplication.", len(full_data))
    return full_data


def record_key(record: dict[str, Any], index: int) -> str:
    return str(
        record.get("rappel_guid")
        or record.get("numero_fiche")
        or record.get("id")
        or f"missing_key_{index}"
    )


def deduplicate_data(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduplicated: dict[str, dict[str, Any]] = {}
    for index, record in enumerate(records):
        deduplicated[record_key(record, index)] = record

    result = list(deduplicated.values())
    logging.info(
        "Deduplicated data: %s records before, %s records after.",
        len(records),
        len(result),
    )
    return result


def query_data() -> list[dict[str, Any]]:
    last_processed = get_latest_timestamp()
    logging.info("Starting API query from last_processed=%s", last_processed)
    return deduplicate_data(get_all_data(last_processed))


def process_data(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "raw_ingested_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def create_kafka_producer():
    KafkaProducer, kafka_errors, Serializer = import_kafka_client_classes()
    serializer = create_json_serializer(Serializer)
    bootstrap_candidates = (
        [server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",") if server.strip()]
        if KAFKA_BOOTSTRAP_SERVERS
        else [LOCAL_KAFKA_BOOTSTRAP_SERVERS, DOCKER_KAFKA_BOOTSTRAP_SERVERS]
    )

    for bootstrap_servers in bootstrap_candidates:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=serializer,
                acks="all",
                retries=5,
            )
            logging.info("Connected to Kafka using %s", bootstrap_servers)
            return producer
        except kafka_errors as error:
            logging.warning("Kafka broker %s unavailable: %s", bootstrap_servers, error)

    raise RuntimeError("Could not connect to any configured Kafka broker")


def stream() -> int:
    records = query_data()
    if not records:
        logging.info("No records to send to Kafka topic %s.", KAFKA_TOPIC)
        return 0

    producer = create_kafka_producer()
    sent_count = 0
    try:
        for record in records:
            producer.send(KAFKA_TOPIC, value=process_data(record)).get(timeout=30)
            sent_count += 1
        producer.flush()
        update_last_processed_file(records)
        logging.info("Sent %s records to Kafka topic %s.", sent_count, KAFKA_TOPIC)
    finally:
        producer.close()
        logging.info("Kafka producer closed.")
    return sent_count


if __name__ == "__main__":
    total_records = stream()
    print(f"Sent {total_records} records to Kafka topic {KAFKA_TOPIC}")
