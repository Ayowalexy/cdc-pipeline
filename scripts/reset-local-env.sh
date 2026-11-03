#!/usr/bin/env bash
# DESTRUCTIVE: removes all containers AND volumes (PostgreSQL rows, Kafka topics/offsets,
# Elasticsearch index), then starts a clean stack. Not a routine stop command; use
# `docker compose stop` / `docker compose down` (without -v) for that.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${1:-}" != "--yes" ]; then
  read -r -p "This deletes all local CDC exercise data (volumes). Continue? [y/N] " answer
  [ "$answer" = "y" ] || [ "$answer" = "Y" ] || { echo "aborted"; exit 1; }
fi

docker compose down -v --remove-orphans
docker compose up -d --build --wait
docker compose ps
