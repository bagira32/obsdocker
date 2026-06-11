#!/usr/bin/env bash
# Import Kibana saved objects (data view, saved searches, "Attack overview"
# dashboard) via the Saved Objects API. Idempotent: re-run any time you change
# kibana/saved-objects.ndjson; --overwrite replaces existing objects in place.
set -euo pipefail

KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NDJSON_FILE="${SCRIPT_DIR}/../kibana/saved-objects.ndjson"

if [[ ! -f "$NDJSON_FILE" ]]; then
  echo "Saved-objects NDJSON not found: $NDJSON_FILE" >&2
  exit 1
fi

echo "Waiting for Kibana at $KIBANA_URL ..."
for i in {1..90}; do
  if curl -sf "$KIBANA_URL/api/status" >/dev/null 2>&1; then break; fi
  sleep 2
done

if ! curl -sf "$KIBANA_URL/api/status" >/dev/null 2>&1; then
  echo "Kibana never became reachable at $KIBANA_URL" >&2
  exit 1
fi

echo "Importing saved objects from $(basename "$NDJSON_FILE") ..."
curl -sf -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
  -H 'kbn-xsrf: true' \
  --form file=@"$NDJSON_FILE" \
  | python3 -m json.tool

echo
echo "Open the dashboard:"
echo "  $KIBANA_URL/app/dashboards#/view/obs-attack-dashboard"
