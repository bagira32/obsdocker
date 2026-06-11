# obsdocker — project tracker for Claude

This file is the durable context for future Claude sessions working in this repo. Keep it accurate. When you make a non-trivial decision, update the relevant section. When something on the **Status** or **Backlog** list changes, update it.

## What this project is

A learning/dev observability stack, **all in docker-compose**, intended to run on a single Linux host (the user's Kali machine). Goal: an end-to-end log pipeline that mirrors a realistic production topology while staying small enough to run locally.

## Pipeline

```
[docker container logs (/var/lib/docker/containers/*/*.log)]
[host journald (/var/log/journal)]                            ┐
[HTTP push   :8888]                                            ├─► fluent-bit ─► kafka (3 brokers, KRaft) ─► kafka-connect (ES sink) ─► elasticsearch ─► kibana
[Fluent Fwd  :24224]                                          ┘                                                                         └─► grafana ◄─┐
                                                                                                                                                      │
[redis]  ─►  redis-exporter  ─►  prometheus  ─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Two datasources in Grafana: **Elasticsearch** (logs, default) and **Prometheus** (metrics). Redis is the first observed metric source; its container logs also flow through the standard fluent-bit tail → `logs` topic path, so the same Redis instance shows up in both pipelines.

Two Kafka topics, both auto-created with 6 partitions / replication 3:
- `logs` — everything from fluent-bit's tail/systemd/forward/http inputs **except** the synthetic generator.
- `flog-logs` — only the synthetic log generator (`mingrammer/flog`), which uses the **docker fluentd log driver** (`tag=app.flog`) to push directly into fluent-bit's forward input, bypassing the JSON-file tail.

Per-source routing in Fluent Bit is by tag matching on the kafka outputs: the main output uses `Match_Regex ^(docker\..*|host\..*|app\.forward|app\.http)$` → topic `logs`; a second output uses `Match app.flog` → topic `flog-logs`. (See the `Match` caveat below for why this isn't a space-separated `Match` list.)

ES targets: two data streams, `logs-fluentbit-logs` and `logs-fluentbit-flog-logs`, both auto-created by the Connect sink (subscribed to both topics). Naming = `{data.stream.type}-{data.stream.dataset}-{topic}` in connector v14.x — the **topic name** is the third segment, not `data.stream.namespace`. We use wildcard `logs-fluentbit-*` everywhere (Grafana, smoke tests) so adding more topics later doesn't require config churn.
ES cluster: 3 nodes (`es01`, `es02`, `es03`), all master+data eligible, cluster name `obs-cluster`. Network alias `elasticsearch` resolves to any healthy node.

## Locked design decisions

| # | Decision | Reasoning |
|---|----------|-----------|
| 1 | Topology = Fluent Bit → Kafka → ES (via Kafka Connect sink), no second FB tier | User pick. Cleaner than dual-FB tier; Connect handles ES indexing concerns. |
| 2 | Inputs = Docker container logs + journald + HTTP + Forward | User pick. Covers the host-level demo and a network-push surface. |
| 3 | ~~Sizing = ES single-node, Kafka 3 brokers~~ **Superseded by #8** | Initial pick for modest footprint. |
| 4 | Security = none (xpack.security disabled, no TLS, no SASL) | User pick. **All host ports bound to 127.0.0.1 only** as the mitigation. |
| 5 | Kafka in KRaft mode (no Zookeeper) | Modern default; one fewer service. |
| 6 | ES index strategy = data stream (not daily indices) | 8.x native; rollover handled by ES. |
| 7 | ES sink installed at container start via `confluent-hub install` (cached on a volume) | Avoids building a custom image; one-time install per volume. |
| 8 | Sizing = **ES 3-node cluster (all master+data)**, Kafka 3 brokers | User pick. Exercises real ES cluster behavior (master election, shard allocation, replicas) at the cost of ~3× ES memory. All three nodes share a docker network alias `elasticsearch` so downstream services can also reach the cluster via a single name. |
| 9 | Metrics pipeline = single Prometheus instance, Grafana as Prometheus client | User pick. Added a parallel metrics path alongside the log pipeline so the stack can observe more than just logs. Redis is the first observed source (via `redis_exporter`); host metrics (`node_exporter`) deferred. |

## Service inventory

| Service       | Image                                          | Internal port | Host port (127.0.0.1) | Notes |
|---------------|------------------------------------------------|---------------|------------------------|-------|
| es01          | elastic/elasticsearch:8.15.3                   | 9200          | 9200                  | master+data, 1g heap, network alias `elasticsearch` |
| es02          | elastic/elasticsearch:8.15.3                   | 9200          | —                     | master+data, 1g heap, network alias `elasticsearch` |
| es03          | elastic/elasticsearch:8.15.3                   | 9200          | —                     | master+data, 1g heap, network alias `elasticsearch` |
| kibana        | elastic/kibana:8.15.3                          | 5601          | 5601                  | multi-host ES config (all three nodes) |
| kafka1/2/3    | confluentinc/cp-kafka:7.7.1                    | 9092 internal / 9093 controller / 19092 external | 19092 / 19093 / 19094 | KRaft, replication 3 |
| kafka-connect | confluentinc/cp-kafka-connect:7.7.1            | 8083          | 8083                  | ES sink 14.1.1 installed at start |
| fluent-bit    | fluent/fluent-bit:3.1.9                        | 24224 / 8888 / 2020 | 24224 / 8888 / 2020 | tail + systemd + forward + http inputs |
| grafana       | grafana/grafana:11.3.0                         | 3000          | 3000                  | ES + Prometheus datasources provisioned, admin/admin |
| kafka-ui      | provectuslabs/kafka-ui:v0.7.2                  | 8080          | 8080                  | view topics, consumer groups, connector |
| redis         | redis:7.4-alpine                               | 6379          | 6379                  | `--save "" --appendonly no` (ephemeral); logs flow via fluent-bit tail → topic `logs` |
| redis-exporter| oliver006/redis_exporter:v1.66.0               | 9121          | 9121                  | scrapes `redis://redis:6379`, exposes `/metrics` for Prometheus |
| prometheus    | prom/prometheus:v3.0.1                         | 9090          | 9090                  | 7d retention, scrapes redis-exporter + itself |
| log-generator | mingrammer/flog:0.4.3                          | -             | -                     | `--profile demo` only; logs via docker fluentd driver (tag=`app.flog`) → topic `flog-logs` |
| redis-benchmark | redis:7.4-alpine                             | -             | -                     | `--profile demo` only; loops `redis-benchmark` against `redis` so metrics aren't flatlined |
| log-injector  | curlimages/curl:8.10.1                         | -             | -                     | `--profile attack` only; POSTs crafted JSON to `fluent-bit:8888` (spoofed host, XSS/SQLi payloads, oversized field, malformed JSON) |
| redis-exploiter | redis:7.4-alpine                             | -             | -                     | `--profile attack` only; drives the unauthenticated-Redis playbook (FLUSHALL, CONFIG SET dir/dbfilename + BGSAVE, refused DEBUG, REPLICAOF) |
| recon         | instrumentisto/nmap:7.95                       | -             | -                     | `--profile attack` only; nmap service-version + NSE HTTP discovery across every named service on the `obs` network |

## Layout

```
obsdocker/
├── .env                                 # versions, heap, bind address, cluster id
├── docker-compose.yml
├── README.md
├── CLAUDE.md                            # this file
├── fluent-bit/
│   ├── fluent-bit.conf                  # 4 inputs, modify filter, kafka output
│   └── parsers.conf                     # docker JSON-file parser
├── kafka-connect/
│   └── elasticsearch-sink.json          # connector config (data-stream mode)
├── kibana/
│   └── saved-objects.ndjson             # data view + 5 attack saved searches + "Attack overview" dashboard
├── grafana/provisioning/
│   ├── datasources/
│   │   ├── elasticsearch.yml
│   │   └── prometheus.yml
│   └── dashboards/
│       ├── dashboards.yml               # provider
│       ├── logs-overview.json
│       ├── redis-overview.json
│       └── attack-overview.json         # `--profile attack` signals (injected events, recon impact, redis abuse)
├── prometheus/
│   └── prometheus.yml                   # scrape config (self + redis-exporter)
├── scripts/
│   ├── register-connector.sh            # idempotent POST/PUT to Connect REST
│   └── import-kibana-objects.sh         # idempotent POST to Kibana Saved Objects API (data view, attack searches, dashboard)
└── .claude/
    └── agents/
        └── obs-doctor.md                # read-only triage subagent for the whole pipeline
```

## Diagnostic subagent

For health checks across the pipeline (compose state, ES cluster, Kafka brokers/topics/lag, Connect, Fluent Bit metrics, recent logs), delegate to the **obs-doctor** subagent (`.claude/agents/obs-doctor.md`). It is read-only — no edits, restarts, or destructive commands — and reports a concise punch list. Invoke it via the `Agent` tool with `subagent_type: obs-doctor` whenever the user asks "is the pipeline working", "what's wrong with X", or to triage a degraded state.

## Running

```bash
docker compose up -d
./scripts/register-connector.sh
./scripts/import-kibana-objects.sh        # data view + attack saved searches + dashboard
# optional demo traffic:
docker compose --profile demo up -d log-generator redis-benchmark
# optional attack simulators (destructive against the local Redis):
docker compose --profile attack up -d log-injector redis-exploiter recon
```

Health-check order is enforced via compose `depends_on: condition: service_healthy`:
- kafka1/2/3 must be healthy before kafka-connect, fluent-bit, kafka-ui start
- elasticsearch must be healthy before kibana, grafana, kafka-connect start

## Status

- [x] Stack scaffolded (compose, fluent-bit, connect sink config, grafana provisioning, starter dashboard, register script, README)
- [x] First `docker compose up` validated end-to-end on this host
- [x] Verified a log line traverses fluent-bit → topic `logs` → data stream `logs-fluentbit-logs` → visible in ES (Kibana/Grafana data view setup still TODO)
- [x] Connector registered cleanly via `scripts/register-connector.sh`
- [ ] Redis metrics pipeline validated end-to-end (redis-exporter scrape → Prometheus → Grafana Redis dashboard renders with non-zero ops/sec under `--profile demo`)

Update this checklist as items are validated.

## Known caveats / things to revisit

- **No auth, no TLS.** Mitigation is `BIND=127.0.0.1` in `.env`. If we ever expose to a LAN, switch to "Basic auth, no TLS" or "Full security" — the original options we discussed.
- **Fluent Bit tails Docker JSON log files directly.** Container names are not extracted yet (only the file path is captured as `source_file`). If we want `kubernetes.container_name`-style metadata, add a filter that joins on container_id, or move to the `forward` driver on dockerd.
- **`container-cached.log` is excluded from the tail input.** Docker writes a protobuf-framed cache file at that name for any container using a non-`json-file`/`journald` driver (e.g. `fluentd`, `local`) so `docker logs` keeps working. Our `docker` parser is JSON-only, so without the exclude those files land in ES as raw framed bytes in the `log` field. Affected containers already ship logs to us via their actual driver (e.g. `log-generator` via the fluentd driver → forward input → `flog-logs`), so excluding the cache is the right call.
- **Self-feedback loop in tail input.** Fluent Bit's own container logs are tailed and re-emitted, so the topic/index is dominated by FB's startup chatter on first boot. Not harmful, just noisy. To exclude, add `Exclude_Path /var/lib/docker/containers/<fluent-bit-id>*/*.log` or filter by container id.
- **ES sink data-stream naming.** The Confluent ES sink v14.x ignores `data.stream.namespace` and uses the **Kafka topic name** as the third segment of the data-stream name. To target a different namespace, either rename the topic or apply a `RegexRouter` SMT in the connector config.
- **Partition key for Kafka producer.** `Message_Key_Field` in fluent-bit.conf is set to `source_file` so docker-tail records spread across all 6 partitions by container id. Records without that field (HTTP/forward inputs) get round-robin partitioning, which is also fine.
- **Forward input must NOT set `Tag`.** Setting `Tag X` on the `[INPUT] forward` block **overrides** any tag supplied by the client (e.g. the docker fluentd log driver's `tag: app.flog`), causing all forward traffic to be re-tagged uniformly. Per-source kafka routing then breaks because every record matches the main output. Keep the forward input tagless so client tags propagate.
- **Restarting Fluent Bit silently breaks the docker fluentd-async producer.** With `fluentd-async=true` on the docker log driver (used by `log-generator`), dockerd buffers in memory and reconnects without surfacing errors. If FB restarts while a batch is in flight, the connection can stay dead and queued records get dropped silently. Symptoms: FB `forward.*` input stuck at 0, `kafka.*` for `flog-logs` at 0, no errors/retries/drops anywhere, but the source container keeps producing fresh logs. Fix: `docker compose --profile demo restart log-generator` to force dockerd to open a fresh forward connection. Any fluentd-driver container needs the same treatment after an FB restart.
- **`Match` takes ONE pattern, not a space-separated list.** Fluent Bit treats the whole `Match` value as a single glob, so `Match docker.* host.* app.forward app.http` silently matches nothing (spaces aren't valid in tags). The failure is invisible — the output registers fine, `proc_records` just stays at 0 with no error/retry/drop. Use `Match_Regex ^(docker\..*|host\..*|app\.forward|app\.http)$` to route multiple tag families into one output.
- **`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`** is convenient for dev. Disable and pre-create `logs` explicitly if we want deterministic partition/replication settings.
- **ES heap is 1g per node × 3 nodes ≈ 3g total.** Drop `ES_HEAP` in `.env` if the host doesn't have the RAM; bump it if indexing slows.
- **`bootstrap.memory_lock=true`** is set on ES so the JVM heap is mlocked. Combined with the `memlock: -1` ulimit; if the kernel refuses to lock memory on this host, set `bootstrap.memory_lock=false` in the `x-es-common` env block.
- **`flush.timeout.ms` / `batch.size` on the ES sink** are conservative; tune if throughput rises.

## Backlog (not started)

- Add a Filebeat → Kafka path for comparison with Fluent Bit
- Add `node_exporter` so Prometheus also has host metrics (Prometheus itself + first observed source landed in decision #9)
- Add an ILM policy / data stream lifecycle for retention
- Add a structured-log demo app (instead of just flog) to exercise level/field mapping
- Add an index template via API at bootstrap (saved searches + attack dashboard now land via `scripts/import-kibana-objects.sh`)

## How to update this file

- **Decision changes** → update the "Locked design decisions" table; do not delete the old row, mark it superseded.
- **New service / version bump** → update "Service inventory" and `.env`.
- **Layout change** → update "Layout" tree.
- **Step completed / regressed** → tick / untick "Status".
- Keep it tight; this file should remain skimmable in one screen scroll.
