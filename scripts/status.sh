#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
docker compose -f docker-compose.yaml ps
docker compose -f docker-compose-airflow.yaml ps
