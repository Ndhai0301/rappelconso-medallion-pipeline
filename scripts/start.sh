#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

docker network inspect airflow-kafka >/dev/null 2>&1 || \
  docker network create airflow-kafka >/dev/null

mkdir -p .ivy2/cache .ivy2/jars data/checkpoints kafka postgres/data
chmod -R u+rwX,g+rwX .ivy2 data kafka postgres

docker compose -f docker-compose.yaml up -d kafka kafka-ui
docker compose -f docker-compose-airflow.yaml up -d --build \
  db airflow-db airflow-init airflow-webserver airflow-scheduler

echo "Airflow:  http://localhost:8080 (admin/admin)"
echo "Kafka UI: http://localhost:8000"
