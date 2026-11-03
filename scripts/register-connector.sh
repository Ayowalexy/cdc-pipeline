#!/usr/bin/env bash
# Idempotently registers (or updates) the Debezium orders connector and waits until it runs.
# Used by the `connect-init` compose service; can also be run from the host:
#   CONNECT_URL=http://localhost:8083 npm run connector:register
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="orders-connector"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${CONNECTOR_CONFIG:-$SCRIPT_DIR/../infrastructure/debezium/orders-connector.json}"

for i in $(seq 1 60); do
  curl -fs "$CONNECT_URL/connectors" > /dev/null && break
  [ "$i" = 60 ] && { echo "Kafka Connect not reachable at $CONNECT_URL" >&2; exit 1; }
  sleep 2
done

# PUT .../config creates the connector or replaces its config: safe to run repeatedly.
code=$(curl -s -o /tmp/connector-put.json -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
  --data @"$CONFIG" "$CONNECT_URL/connectors/$NAME/config")
if [ "$code" != 200 ] && [ "$code" != 201 ]; then
  echo "connector registration failed (HTTP $code):" >&2
  cat /tmp/connector-put.json >&2
  exit 1
fi
echo "connector $NAME registered (HTTP $code)"

for i in $(seq 1 60); do
  status=$(curl -fs "$CONNECT_URL/connectors/$NAME/status" || true)
  if echo "$status" | grep -q '"tasks":\[{"id":0,"state":"RUNNING"'; then
    echo "connector $NAME and task 0 are RUNNING"
    exit 0
  fi
  if echo "$status" | grep -q '"state":"FAILED"'; then
    echo "connector $NAME FAILED: $status" >&2
    exit 1
  fi
  sleep 2
done
echo "connector $NAME did not reach RUNNING: $status" >&2
exit 1
