#!/usr/bin/env bash
# Register (or update) the Elasticsearch sink connector against Kafka Connect.
# Idempotent: re-run any time you change kafka-connect/elasticsearch-sink.json.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../kafka-connect/elasticsearch-sink.json"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Connector config not found: $CONFIG_FILE" >&2
  exit 1
fi

echo "Waiting for Kafka Connect at $CONNECT_URL ..."
for i in {1..60}; do
  if curl -sf "$CONNECT_URL/" >/dev/null; then break; fi
  sleep 2
done

name=$(python3 -c "import json,sys; print(json.load(open('$CONFIG_FILE'))['name'])")
cfg=$(python3 -c "import json,sys; print(json.dumps(json.load(open('$CONFIG_FILE'))['config']))")

if curl -sf "$CONNECT_URL/connectors/$name" >/dev/null 2>&1; then
  echo "Connector '$name' exists — updating config ..."
  curl -sf -X PUT -H 'Content-Type: application/json' \
    --data "$cfg" \
    "$CONNECT_URL/connectors/$name/config" \
    | python3 -m json.tool
else
  echo "Creating connector '$name' ..."
  curl -sf -X POST -H 'Content-Type: application/json' \
    --data @"$CONFIG_FILE" \
    "$CONNECT_URL/connectors" \
    | python3 -m json.tool
fi

echo
echo "Connector status:"
curl -sf "$CONNECT_URL/connectors/$name/status" | python3 -m json.tool
