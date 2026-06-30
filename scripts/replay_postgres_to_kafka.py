"""Rebuild the raw Kafka topic from PostgreSQL's preserved raw_data column."""

import json
import logging

from src.kafka_client.kafka_stream_data import KAFKA_TOPIC, create_kafka_producer
from src.postgres_client.migrate import connect


def replay() -> int:
    producer = create_kafka_producer()
    sent = 0
    try:
        with connect() as connection:
            with connection.cursor(name="rappelconso_replay") as cursor:
                cursor.itersize = 1000
                cursor.execute(
                    "SELECT raw_data::text FROM dev.rappelconso_raw "
                    "WHERE raw_data IS NOT NULL ORDER BY db_id"
                )
                for (raw_data,) in cursor:
                    producer.send(KAFKA_TOPIC, value=json.loads(raw_data))
                    sent += 1
        producer.flush()
    finally:
        producer.close()

    logging.info("Replayed %s rows to Kafka topic %s", sent, KAFKA_TOPIC)
    return sent


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    replay()
