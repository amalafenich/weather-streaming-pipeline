#!/usr/bin/env bash
set -euo pipefail

docker compose exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic weather-data \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1

