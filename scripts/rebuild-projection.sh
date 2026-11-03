#!/usr/bin/env bash
# Rebuilds the Elasticsearch `orders` projection from the CDC topic.
#
# Required once when deploying the version-guard fix onto an index written by the old consumer:
# those documents carry Elasticsearch's internal write counter as _version, which external
# versioning compares against, so newer legitimate updates would be rejected as stale.
#
# Steps: stop consumer -> delete index -> rewind consumer group to earliest -> start consumer
# (which recreates the index with explicit mappings) -> wait until converged with PostgreSQL.
#
# Requires the topic to still hold every event since the first one (local topic: retention.ms=-1,
# see kafka-init). Elasticsearch data only; PostgreSQL and Kafka are untouched.
set -euo pipefail
cd "$(dirname "$0")/.."

INDEX="${ES_ORDERS_INDEX:-orders}"
ES="http://localhost:${ES_HOST_PORT:-9200}"
GROUP="${KAFKA_GROUP_ID:-order-projection}"
TOPIC="${KAFKA_ORDERS_TOPIC:-cdc.public.orders}"

docker compose stop cdc-consumer
curl -fsS -X DELETE "$ES/$INDEX?ignore_unavailable=true" && echo
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group "$GROUP" --topic "$TOPIC" --reset-offsets --to-earliest --execute
docker compose up -d --no-deps --wait cdc-consumer
npx tsx scripts/verify-convergence.ts "$@"
