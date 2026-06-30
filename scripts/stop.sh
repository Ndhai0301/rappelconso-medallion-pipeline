#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
docker compose -f docker-compose-airflow.yaml down
docker compose -f docker-compose.yaml down
