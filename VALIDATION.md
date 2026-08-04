# obsdocker — step-by-step validation

A per-service checklist for confirming the whole stack is actually working, not just "up". Each section: what it proves, the exact command(s), and what a healthy result looks like. Run sections in order — later ones assume earlier ones passed.

Credentials/ports referenced below come from `.env` (`BIND=127.0.0.1`, `ELASTIC_PASSWORD=changeme`, Grafana `admin/admin`). Postgres's host port is **15432**, not the default 5432 — this Kali host already runs several native Postgres clusters on 5432-5436.

## 0. Prerequisites

```bash
cd /home/bagira/AIPlayground/obsdocker
docker compose up -d
./scripts/register-connector.sh
./scripts/import-kibana-objects.sh
```

```bash
# Every service should show "healthy" (or no healthcheck defined — kafka-ui, kibana,
# grafana, fluent-bit, prometheus don't define one, so they'll show plain "Up")
docker compose ps --format '{{.Service}}: {{.Status}}' | sort
```
Expect all of: `es01/02/03`, `kafka1/2/3`, `kafka-connect`, `postgres`, `redis` marked `(healthy)`; everything else just `Up`.

---

## 1. Elasticsearch (3-node cluster)

Proves: cluster formed, all 3 nodes joined, status is not red.

```bash
curl -s -u elastic:changeme 'http://localhost:9200/_cluster/health?pretty'
```
Expect `"status": "green"` or `"yellow"` and `"number_of_nodes": 3`.

```bash
curl -s -u elastic:changeme 'http://localhost:9200/_cat/nodes?v'
```
Expect 3 rows (`es01`, `es02`, `es03`), each with a `master` flag on at least one (`m`/`*`).

---

## 2. Kafka (3 brokers, KRaft)

Proves: brokers formed a cluster and the `logs`/`flog-logs` topics exist with the expected partition/replication config.

```bash
docker exec kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092 | grep -c ':9092'
```
Expect `3` (one line per broker).

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 --describe --topic logs
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 --describe --topic flog-logs
```
Expect `PartitionCount: 6`, `ReplicationFactor: 3` for both.

---

## 3. Kafka Connect (Elasticsearch sink)

Proves: the connector installed, registered, and both tasks are running (not `FAILED`).

```bash
curl -s http://localhost:8083/connectors/logs-es-sink/status | jq
```
Expect `"state": "RUNNING"` for the connector and for every task in `"tasks": [...]`.

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group connect-logs-es-sink
```
Expect no growing `LAG` column over repeated runs (a few seconds apart) once traffic is steady — a persistently large/growing lag means the sink can't keep up or ES is rejecting writes.

---

## 4. Fluent Bit

Proves: inputs are receiving records and outputs are shipping them (not stuck at 0 — see the `Match`/`Match_Regex` caveat in CLAUDE.md if so).

```bash
curl -s http://localhost:2020/api/v1/metrics | jq '.input | to_entries[] | {name: .key, records: .value.records}'
curl -s http://localhost:2020/api/v1/metrics | jq '.output | to_entries[] | {name: .key, records: .value.proc_records, retries: .value.retries_failed}'
```
Expect non-zero `records` on the `tail.*`/`systemd.*` inputs, non-zero `proc_records` on `kafka.0` (the main `logs` output), and `retries_failed: 0` everywhere. `kafka.1` (the `flog-logs` output, matching only `app.flog`) stays at `0` until `log-generator` is started under `--profile demo` (§13) — that's expected, not a failure.

---

## 5. End-to-end log flow (HTTP push → Kafka → ES)

Proves the full pipeline in one shot with a traceable marker.

```bash
MARKER="probe-$(date +%s)"
curl -X POST -H 'Content-Type: application/json' \
     -d "{\"log\":\"$MARKER\",\"level\":\"info\"}" \
     http://localhost:8888/test
sleep 10
curl -s -u elastic:changeme "http://localhost:9200/logs-fluentbit-*/_search?pretty" \
     -H 'Content-Type: application/json' -d "{
       \"query\": { \"match_phrase\": { \"log\": \"$MARKER\" } }
     }"
```
Expect `"hits": {"total": {"value": 1, ...}}` with your marker in `_source.log`.

---

## 6. Kibana

Proves: Kibana can reach the ES cluster and the saved objects imported cleanly.

```bash
curl -s -u elastic:changeme http://localhost:5601/api/status | jq '.status.overall.level'
```
Expect `"available"`.

```bash
curl -s -u elastic:changeme 'http://localhost:5601/api/saved_objects/_find?type=dashboard' | jq '.saved_objects[] | {id, title: .attributes.title}'
```
Expect the `obs-attack-dashboard` entry (imported by `scripts/import-kibana-objects.sh`).

Manual check: open http://localhost:5601 → Discover → data view `logs-fluentbit-*` → confirm recent hits.

---

## 7. Redis + redis-exporter

Proves: Redis answers, and the exporter is translating `INFO` into Prometheus metrics.

```bash
docker exec redis redis-cli ping
```
Expect `PONG`.

```bash
curl -s http://localhost:9121/metrics | grep -E '^redis_(up|connected_clients) '
```
Expect `redis_up 1`.

---

## 8. Prometheus

Proves: every scrape target is healthy — this is the fastest single check for the whole metrics half of the stack.

```bash
curl -s 'http://localhost:9090/api/v1/targets?state=active' | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'
```
Expect `health: "up"` for jobs `prometheus`, `redis`, and `products-api`.

---

## 9. Grafana

Proves: both datasources are provisioned and both dashboards are registered and queryable.

```bash
curl -s -u admin:admin http://localhost:3000/api/datasources | jq '.[] | {name, type, uid}'
```
Expect an `elasticsearch` entry and a `prometheus` entry (`uid: prometheus`).

```bash
curl -s -u admin:admin 'http://localhost:3000/api/search?type=dash-db' | jq '.[] | {uid, title}'
```
Expect at least: `Logs overview`, `Redis overview`, `Products API overview`, `Attack overview`.

```bash
# Simulate a panel query through Grafana's own datasource proxy (not direct-to-Prometheus)
curl -s -u admin:admin -H 'Content-Type: application/json' -X POST http://localhost:3000/api/ds/query -d '{
  "queries": [{"refId":"A","datasource":{"type":"prometheus","uid":"prometheus"},"expr":"up","instant":true}],
  "from": "now-5m", "to": "now"
}' | jq '.results.A.frames[0].data.values'
```
Expect a non-empty `values` array (timestamps + `1`s). Manual check: open http://localhost:3000/dashboards and confirm panels render — this project's headless browser testing tool cannot composite frames for a screenshot, so a real browser is the only way to eyeball the panels.

---

## 10. Kafka UI

Proves: reachable and can see the cluster + connector.

```bash
curl -s http://localhost:8080/api/clusters | jq '.[] | {name, status}'
```
Expect `"status": "online"` for cluster `obs`.

Manual check: http://localhost:8080 → cluster `obs` → Topics (see `logs`, `flog-logs`) → Kafka Connect (see `logs-es-sink`, `RUNNING`).

---

## 11. Postgres

Proves: reachable, schema applied, seed rows present.

```bash
docker exec postgres pg_isready -U products -d products
docker exec postgres psql -U products -d products -c 'SELECT count(*) FROM products;'
```
Expect `accepting connections` and a row count ≥ 5 (5 seeded rows, plus any created since).

---

## 12. products-api (FastAPI + cache-aside)

Proves: CRUD works, the cache-aside pattern actually hits/misses/invalidates Redis correctly, and its own telemetry (metrics + logs) reaches the rest of the stack.

```bash
# health + seeded data
curl -s http://localhost:8000/health
curl -s http://localhost:8000/products | jq -c '.[] | {id,name}' | head -5
```

```bash
# create, then prove miss -> hit -> invalidate -> delete -> 404
NEW=$(curl -s -X POST -H 'content-type: application/json' \
      -d '{"name":"Validation","price":9.99,"stock":1}' http://localhost:8000/products)
ID=$(echo "$NEW" | jq -r .id)

curl -s "http://localhost:8000/products/$ID" >/dev/null   # miss, populates cache
docker exec redis redis-cli EXISTS product:$ID             # expect 1
docker exec redis redis-cli TTL product:$ID                # expect ~30

curl -s "http://localhost:8000/products/$ID" | jq -c        # hit, served from cache

curl -s -X PUT -H 'content-type: application/json' \
     -d '{"name":"Validation2","price":10.99,"stock":2}' \
     "http://localhost:8000/products/$ID" >/dev/null
docker exec redis redis-cli EXISTS product:$ID              # expect 0 (invalidated)

curl -s -o /dev/null -w '%{http_code}\n' -X DELETE "http://localhost:8000/products/$ID"   # expect 204
curl -s -o /dev/null -w '%{http_code}\n' "http://localhost:8000/products/$ID"             # expect 404
```

```bash
# metrics reaching Prometheus
curl -s http://localhost:8000/metrics | grep -E '^products_api_cache_(hits|misses)_total'
curl -s --data-urlencode 'query=products_api_cache_hits_total' http://localhost:9090/api/v1/query | jq -c '.data.result'
```

```bash
# logs reaching Elasticsearch (zero pipeline config — auto-tailed like every other container)
CID=$(docker inspect -f '{{.Id}}' products-api)
curl -s -u elastic:changeme "http://localhost:9200/logs-fluentbit-logs/_search?pretty" \
  -H 'Content-Type: application/json' -d "{\"query\":{\"wildcard\":{\"source_file\":\"*${CID}*\"}},\"size\":1,\"sort\":[{\"@timestamp\":\"desc\"}]}"
```
Expect a hit whose `_source.log` is a JSON line with `"name": "products_api"`.

---

## 13. Demo traffic (optional, `--profile demo`)

Proves: the demo generators actually produce sustained load so the dashboards aren't flatlined.

```bash
docker compose --profile demo up -d log-generator redis-benchmark products-traffic
sleep 30
```

```bash
# flog traffic reaching its own topic
docker exec kafka1 kafka-get-offsets --bootstrap-server localhost:9092 --topic flog-logs --time -1
# non-zero ops/sec on redis
curl -s --data-urlencode 'query=rate(redis_commands_processed_total[1m])' http://localhost:9090/api/v1/query | jq -r '.data.result[0].value[1]'
# non-zero request rate on products-api
curl -s --data-urlencode 'query=sum(rate(http_requests_total{handler=~"/products.*"}[1m]))' http://localhost:9090/api/v1/query | jq -r '.data.result[0].value[1]'
```
Expect non-zero offsets/rates on all three.

**Heads up:** `products-traffic` POSTs a new row roughly every 2s. Postgres persists (unlike Redis), so the `products` table grows unbounded while it runs — stop it when you're done validating:
```bash
docker compose --profile demo stop products-traffic
```

---

## 14. Attack simulators (optional, `--profile attack`, destructive to local Redis)

Proves: the intentionally-bad-actor containers actually generate the signals the "Attack overview" dashboards are built to show. Only run this after you've confirmed 0-13 above, and note it will `FLUSHALL` the local Redis repeatedly — see the "Heads up" callout in `README.md` § Attack simulators before starting.

```bash
docker compose --profile attack up -d log-injector redis-exploiter recon
sleep 30
```

```bash
# spoofed/injected events landing in ES
curl -s -u elastic:changeme 'http://localhost:9200/logs-fluentbit-logs/_count?pretty' \
  -H 'Content-Type: application/json' -d '{"query":{"query_string":{"query":"host:(kibana OR es01 OR app) OR _exists_:blob OR _exists_:ua"}}}'
# redis ops/sec spiking from the exploiter's pipelined SETs
curl -s --data-urlencode 'query=rate(redis_commands_processed_total[1m])' http://localhost:9090/api/v1/query | jq -r '.data.result[0].value[1]'
# recon (nmap NSE) lines
curl -s -u elastic:changeme 'http://localhost:9200/logs-fluentbit-logs/_count?pretty' \
  -H 'Content-Type: application/json' -d '{"query":{"match_phrase":{"log":"Nmap Scripting Engine"}}}'
```
Expect non-zero counts on all three. Tear down when done:
```bash
docker compose --profile attack stop log-injector redis-exploiter recon
docker compose --profile attack rm -f log-injector redis-exploiter recon
docker exec redis redis-cli FLUSHALL   # clean keyspace afterwards
```

---

## Pass/fail summary template

Copy this into a scratch note while running through the doc:

```
[ ] 0  Prerequisites — all core services healthy
[ ] 1  Elasticsearch cluster green/yellow, 3 nodes
[ ] 2  Kafka 3 brokers, logs/flog-logs 6x3
[ ] 3  Kafka Connect sink RUNNING, no growing lag
[ ] 4  Fluent Bit inputs/outputs non-zero, no retries_failed
[ ] 5  End-to-end marker traverses HTTP -> Kafka -> ES
[ ] 6  Kibana available, saved objects imported
[ ] 7  Redis PONG, redis_up 1
[ ] 8  Prometheus targets all "up"
[ ] 9  Grafana datasources + dashboards present, panel query returns data
[ ] 10 Kafka UI cluster online
[ ] 11 Postgres accepting connections, products seeded
[ ] 12 products-api CRUD + cache-aside + metrics + logs all correct
[ ] 13 (optional) demo traffic non-zero on flog/redis/products-api
[ ] 14 (optional) attack signals visible in ES + Prometheus
```
