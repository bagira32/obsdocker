# obsdocker — End-to-End Stack Deep Dive

This document explains every component of the observability stack, why it is configured the way it is, and how all the pieces connect. Reading it top to bottom should give you enough understanding to rebuild the stack from scratch and to reason about problems when something breaks.

---

## Table of Contents

1. [What the stack does](#1-what-the-stack-does)
2. [Architecture overview](#2-architecture-overview)
3. [Docker networking and port strategy](#3-docker-networking-and-port-strategy)
4. [Fluent Bit — log collection and routing](#4-fluent-bit--log-collection-and-routing)
   - [Auditd input and parser](#5-auditd-tail-input)
5. [Kafka — the message bus](#5-kafka--the-message-bus)
6. [Kafka Connect — moving data into Elasticsearch](#6-kafka-connect--moving-data-into-elasticsearch)
7. [Elasticsearch — storage and search](#7-elasticsearch--storage-and-search)
8. [Kibana — visualization and detection](#8-kibana--visualization-and-detection)
   - [Auditd detection rules](#auditd-detection-rules)
9. [Prometheus and Redis Exporter — metrics pipeline](#9-prometheus-and-redis-exporter--metrics-pipeline)
10. [Grafana — unified dashboards](#10-grafana--unified-dashboards)
11. [Redis — the observed service](#11-redis--the-observed-service)
12. [Attack simulation profile](#12-attack-simulation-profile)
13. [Security model](#13-security-model)
14. [Startup order and health checks](#14-startup-order-and-health-checks)
15. [Running the stack](#15-running-the-stack)
16. [Troubleshooting reference](#16-troubleshooting-reference)

---

## 1. What the stack does

The stack is a self-contained, single-host observability platform that demonstrates a realistic production-style log and metrics pipeline. It ingests logs from multiple sources, routes them through a message queue, indexes them in a distributed search engine, and visualises both logs and metrics in a unified dashboard.

Two parallel pipelines run simultaneously:

- **Log pipeline**: container logs + host journal + **auditd** + HTTP push → Fluent Bit → Kafka → Kafka Connect → Elasticsearch → Kibana / Grafana
- **Metrics pipeline**: Redis → redis-exporter → Prometheus → Grafana

A third optional pipeline (the `--profile attack`) generates adversarial signals — SQL injection, XSS, Redis abuse, port scanning — to populate detection rules in Kibana Security.

---

## 2. Architecture overview

```
[Docker container logs  /var/lib/docker/containers/*/*.log ]
[Host journald          /var/log/journal                   ]
[Host auditd            /var/log/audit/audit.log           ]  ──► fluent-bit :24224/:8888/:2020
[Forward protocol       :24224                             ]            │
[HTTP push              :8888                              ]            │  tag routing
                                                                        ▼
                                                           kafka1/kafka2/kafka3 (KRaft, 3 brokers)
                                                                topics: logs, flog-logs
                                                                        │
                                                                        ▼
                                                               kafka-connect (ES sink)
                                                                        │
                                                         ┌──────────────┴──────────────┐
                                                         ▼                             ▼
                                             data stream:                   data stream:
                                         logs-fluentbit-logs          logs-fluentbit-flog-logs
                                                         │                             │
                                                         └──────────────┬──────────────┘
                                                                        ▼
                                                           es01 / es02 / es03 (cluster)
                                                                        │
                                                               ┌────────┴────────┐
                                                               ▼                 ▼
                                                            kibana            grafana ◄── prometheus ◄── redis-exporter ◄── redis
```

---

## 3. Docker networking and port strategy

### The `obs` bridge network

All services share a single user-defined bridge network named `obs`. On a user-defined bridge:

- Containers can reach each other by **container name** (Docker's embedded DNS resolves `kafka1`, `es01`, `fluent-bit`, etc.)
- Containers are isolated from other Docker networks by default
- You can assign multiple DNS aliases to a container (used for the `elasticsearch` alias below)

### The `elasticsearch` network alias

All three ES nodes (`es01`, `es02`, `es03`) share the alias `elasticsearch` on the `obs` network:

```yaml
networks:
  obs:
    aliases:
      - elasticsearch
```

This means any container that connects to `http://elasticsearch:9200` gets load-balanced to a healthy ES node by Docker's DNS round-robin. Downstream services that only need to reach ES without caring which node (e.g. the Kibana health check) use this alias.

### Host port binding (`BIND=127.0.0.1`)

Every port published to the host is bound to `127.0.0.1` only:

```
"${BIND}:9200:9200"   →   127.0.0.1:9200:9200
```

This means the stack is only reachable from the local machine. A remote attacker on the LAN cannot reach Kibana, Kafka, or Elasticsearch even though none of them require authentication to the browser (Kibana uses `kibana_system` internally but presents a login form). This is the primary security mitigation for running without TLS on the HTTP layer.

---

## 4. Fluent Bit — log collection and routing

Fluent Bit is the log collector. It runs as a container with the Docker socket and `/var/lib/docker/containers` mounted in, which gives it access to every other container's log files.

### How Fluent Bit processes data

Fluent Bit processes records through a pipeline: **Input → (Parser) → Filter → Output**. Each record carries a **tag** — a string that controls which outputs receive it. This tag-based routing is the key concept.

### Inputs

#### 1. Tail (Docker JSON log files)

```ini
[INPUT]
    Name              tail
    Tag               docker.*
    Path              /var/lib/docker/containers/*/*.log
    Exclude_Path      /var/lib/docker/containers/*/container-cached.log
    Path_Key          source_file
    Parser            docker
    DB                /var/lib/fluent-bit/flb-docker.db
```

- **What it does**: watches every `*.log` file under Docker's container log directory and tails new lines as they appear.
- **Tag**: `docker.*` where `*` is replaced with the file path (e.g. `docker.var.lib.docker.containers.abc123.abc123-json.log`). The dot-separated path uniquely identifies the source container.
- **`Exclude_Path`**: Docker writes a `container-cached.log` file for any container using a non-`json-file` log driver (e.g. `fluentd`, `local`). This is a protobuf-framed binary file, not JSON. Without the exclude, Fluent Bit's JSON parser would produce garbage in the `log` field. Containers using these drivers already send logs via their actual driver path (e.g. `log-generator` uses the fluentd driver → forward input), so excluding the cache file is correct.
- **`Path_Key: source_file`**: adds the log file path as a field named `source_file` on every record. This is used as the Kafka partition key so records from the same container always go to the same partition (ordering guarantee).
- **`DB`**: a SQLite database that stores the position (byte offset) of each tailed file. This survives Fluent Bit restarts and prevents re-reading already-processed lines.
- **Parser**: `docker` (see parsers.conf) — extracts the JSON structure `{"log":"...","stream":"stdout","time":"..."}` and promotes `time` to the record timestamp.

#### 2. Systemd journal

```ini
[INPUT]
    Name              systemd
    Tag               host.systemd
    Path              /var/log/journal
    Read_From_Tail    On
    Strip_Underscores On
```

- **What it does**: reads from the host's journald binary journal files. Every systemd unit, kernel message, and daemon log lands here.
- **`Strip_Underscores`**: journald field names begin with `_` (e.g. `_PID`, `_COMM`). Stripping them produces cleaner field names (`PID`, `COMM`) in Elasticsearch.
- **`Read_From_Tail`**: only processes journal entries written after Fluent Bit starts, not the full history.

#### 3. Forward protocol

```ini
[INPUT]
    Name              forward
    Listen            0.0.0.0
    Port              24224
```

- **What it does**: implements the Fluentd forward protocol. Clients (including the Docker `fluentd` log driver) connect to this port and push structured log records directly.
- **No `Tag` directive**: this is critical. If you set `Tag X` here, Fluent Bit overrides whatever tag the client sent. The `log-generator` container uses the Docker fluentd log driver with `tag: app.flog`. Without a `Tag` directive on this input, that tag propagates intact through the pipeline and allows per-source routing to the `flog-logs` Kafka topic.

#### 4. HTTP input

```ini
[INPUT]
    Name              http
    Tag               app.http
    Listen            0.0.0.0
    Port              8888
```

- **What it does**: accepts arbitrary JSON bodies over HTTP POST. Any application (or the `log-injector` attack container) can push a JSON object to port 8888 and it becomes a log record.
- **Tag**: always `app.http` regardless of what the POST contains.

#### 5. Auditd tail input

```ini
[INPUT]
    Name              tail
    Tag               host.audit
    Path              /var/log/audit/audit.log
    Path_Key          source_file
    Parser            auditd
    DB                /var/lib/fluent-bit/flb-audit.db
    Mem_Buf_Limit     32MB
    Skip_Long_Lines   On
    Read_from_Head    Off
    Refresh_Interval  5
```

- **What it does**: tails the Linux audit daemon's log file (`/var/log/audit/audit.log`) and parses each line into structured fields.
- **Tag**: `host.audit` — matches the `host\..*` branch of the Kafka output's `Match_Regex`, so auditd records flow to the `logs` topic without any routing changes.
- **Volume mount required**: `/var/log/audit` must be bind-mounted read-only into the Fluent Bit container. The directory is owned by `root:adm` (`rwxr-x---`); the Fluent Bit container runs as root so it can read the files.
- **`DB`**: separate SQLite position database from the Docker tail input (`flb-audit.db` vs `flb-docker.db`). Each tail input needs its own DB file — sharing a DB across inputs causes position tracking corruption.
- **`Read_from_Head: Off`**: only processes lines written after Fluent Bit starts. The audit log rotates frequently (every ~8 MB by default); older rotated files (`audit.log.1`, `.2`, etc.) are deliberately skipped to avoid backfilling months of history on first start.

**Why auditd and not just journald?** The `systemd` input already captures journald. However, on most Linux distributions, auditd writes directly to `/var/log/audit/audit.log` rather than routing through journald — the two logging subsystems are independent. The `audisp-journal` plugin can bridge them, but without it, journald has no audit records. Tailing the file directly is the reliable path.

### Parsers (parsers.conf)

#### Docker parser

```ini
[PARSER]
    Name        docker
    Format      json
    Time_Key    time
    Time_Format %Y-%m-%dT%H:%M:%S.%L
    Time_Keep   On
```

Docker writes logs in the format:
```json
{"log":"actual log line\n","stream":"stdout","time":"2024-01-01T00:00:00.123456789Z"}
```

The parser:
- Parses the outer JSON
- Extracts `time` as the record timestamp
- `Time_Keep: On` preserves `time` as a field in addition to setting the record timestamp (so `@timestamp` in ES reflects actual log time, not ingest time)

#### Auditd parser

```ini
[PARSER]
    Name        auditd
    Format      regex
    Regex       ^type=(?<audit_type>[^ ]+) msg=audit\((?<ts>[0-9]+)\.[0-9]+:(?<audit_serial>[0-9]+)\): ?(?<message>.*)$
    Time_Key    ts
    Time_Format %s
```

Auditd log lines follow this format:
```
type=SYSCALL msg=audit(1750000000.123:456): arch=c000003e syscall=59 success=yes exit=0 ... key="execve"
```

The regex extracts:
- **`audit_type`**: the record type — `SYSCALL`, `EXECVE`, `PATH`, `CWD`, `USER_AUTH`, `USER_LOGIN`, `USER_CMD`, `ADD_USER`, `DEL_USER`, `DAEMON_END`, etc. This is the primary field for detection rule queries.
- **`ts`**: the Unix epoch seconds from the `msg=audit(...)` header, promoted to `@timestamp` in Elasticsearch. The sub-second fractional part is dropped by the regex (auditd's precision is milliseconds but not needed for detection).
- **`audit_serial`**: the serial number inside the parentheses. A single kernel event (e.g. one `execve` syscall) generates multiple audit records — a `SYSCALL` record, an `EXECVE` record with the argument list, one or more `PATH` records for each file involved, and a `CWD` record. All records from the same event share the same serial number, enabling correlation in ES.
- **`message`**: the raw key-value payload after the header. Searchable as a full-text field in Kibana — e.g. `message: *res=failed*` or `message: *key=execve*`.

**Auditd record types relevant for detection:**

| Type | Trigger | Key fields in `message` |
|---|---|---|
| `USER_AUTH` | PAM authentication attempt | `res=success\|failed`, `acct="username"`, `exe="/usr/sbin/sshd"` |
| `USER_LOGIN` | Successful login | `uid=`, `pid=`, `exe=` |
| `USER_CMD` | sudo invocation | `cmd=` (the full command), `terminal=`, `res=` |
| `SYSCALL` | System call matching an audit rule | `syscall=` (number), `success=`, `exe=`, `pid=`, `uid=` |
| `EXECVE` | Arguments of an execve syscall | `argc=`, `a0=`, `a1=`, ... (each argument) |
| `PATH` | File path involved in a syscall | `name=` (path), `inode=`, `mode=` |
| `ADD_USER` | `useradd`/`adduser` invocation | `acct="newuser"`, `exe=` |
| `DEL_USER` | `userdel` invocation | `acct="deleteduser"`, `exe=` |
| `CONFIG_CHANGE` | `auditctl` rule change | new/old rule text |
| `DAEMON_END` | auditd process exit | `reason=` |

### Filter

```ini
[FILTER]
    Name    modify
    Match   *
    Add     host_source obsdocker
```

Adds a constant field `host_source: obsdocker` to every record from every input. This is a lightweight breadcrumb — in a multi-host environment you would set this to the hostname so you can filter by source machine in Kibana.

### Outputs and tag routing

This is where the routing decision happens.

#### Output 1 — main `logs` topic

```ini
[OUTPUT]
    Name        kafka
    Match_Regex ^(docker\..*|host\..*|app\.forward|app\.http)$
    Topics      logs
    ...
```

`Match_Regex` accepts a regular expression against the record's tag. This routes:
- All Docker container log records (`docker.*`)
- All host journal records (`host.*`)
- Forward input records tagged `app.forward`
- HTTP input records tagged `app.http`

**Why `Match_Regex` instead of `Match`?** Fluent Bit's `Match` field takes a single glob pattern — not a space-separated list. `Match docker.* host.*` would be interpreted as a literal string containing a space, which matches nothing. `Match_Regex` allows a proper regular expression with alternation (`|`).

#### Output 2 — `flog-logs` topic

```ini
[OUTPUT]
    Name    kafka
    Match   app.flog
    Topics  flog-logs
    ...
```

Routes only the synthetic log generator's output to its own topic. The `log-generator` container uses the Docker fluentd driver with `tag: app.flog`, so its records arrive at the forward input with that tag and are matched here.

#### Kafka output settings

```ini
rdkafka.compression.type snappy
rdkafka.acks             all
```

- **`snappy` compression**: Snappy is a fast, moderate-ratio compression algorithm. Log data is highly compressible; this reduces network bandwidth between Fluent Bit and Kafka brokers significantly.
- **`acks: all`**: the producer waits for all in-sync replicas to acknowledge before considering a write successful. Combined with `min.insync.replicas=2` on the Kafka side, this means a log record is only acknowledged after it's on at least 2 of 3 broker disks. This prevents data loss if a broker dies immediately after a write.

### Fluent Bit built-in HTTP server (monitoring)

```ini
HTTP_Server   On
HTTP_Listen   0.0.0.0
HTTP_Port     2020
```

Fluent Bit exposes its own metrics at `http://fluent-bit:2020/api/v1/metrics`. Useful fields:

- `input.records` — total records ingested per input
- `output.proc_records` — records successfully sent per output
- `output.retried_records` — records that required retry
- `output.dropped_records` — records permanently lost

If `proc_records` on a Kafka output is stuck at 0 while the input is incrementing, the routing tag regex does not match — check `Match_Regex`.

---

## 5. Kafka — the message bus

Kafka decouples the log producer (Fluent Bit) from the log consumer (Kafka Connect / Elasticsearch). Benefits:

- **Buffering**: if Elasticsearch is slow or temporarily unavailable, Kafka holds records. Fluent Bit keeps writing; Connect catches up when ES recovers.
- **Replay**: Kafka retains records for a configurable period. You can re-process old data.
- **Fan-out**: multiple consumers can independently read the same topic (e.g. add a second consumer for alerting without touching the ES pipeline).

### KRaft mode (no ZooKeeper)

Kafka traditionally required ZooKeeper to manage cluster metadata and leader election. Kafka 2.8+ introduced KRaft (Kafka Raft) — Kafka manages its own metadata using the Raft consensus algorithm. This eliminates ZooKeeper as a dependency, simplifying the topology (one fewer service to run, monitor, and version-match).

Each broker is configured with:
```yaml
KAFKA_PROCESS_ROLES: 'broker,controller'
```

Each broker plays both roles: it participates in the Raft quorum for controller elections AND handles producer/consumer requests. The quorum voters are declared explicitly:

```yaml
KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka1:9093,2@kafka2:9093,3@kafka3:9093'
```

Format: `node_id@hostname:controller_port`. All three brokers vote on leader elections. A majority quorum (2 of 3) is required for any leadership decision.

### Listener configuration

Each broker has three listeners on three ports:

| Listener name | Port | Used by |
|---|---|---|
| `INTERNAL` | 9092 | Other Kafka brokers (replication), internal clients (Fluent Bit, Connect, kafka-ui) |
| `CONTROLLER` | 9093 | KRaft controller communication only |
| `EXTERNAL` | 19092/19093/19094 | Clients on the host machine (e.g. `kafka-console-consumer` from the terminal) |

The listener security protocol map sets all three to plaintext:
```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT'
```

Advertised listeners tell clients what address to use for subsequent connections:
```yaml
# kafka1:
KAFKA_ADVERTISED_LISTENERS: 'INTERNAL://kafka1:9092,EXTERNAL://localhost:19092'
```

When a producer connects and asks "where is the leader for partition 0?", Kafka responds with the INTERNAL address (`kafka1:9092`) for Docker-network clients and the EXTERNAL address (`localhost:19092`) for host-side clients.

### Topics

Two topics are auto-created:

| Topic | Partitions | Replication factor | Source |
|---|---|---|---|
| `logs` | 6 | 3 | Fluent Bit tail + systemd + forward + http inputs |
| `flog-logs` | 6 | 3 | log-generator via docker fluentd driver |

**Why 6 partitions?** Partitions are the unit of parallelism. Kafka Connect can run up to `tasks.max=2` concurrent sink tasks; each task consumes from a subset of partitions. Six partitions allow future scaling to more tasks without repartitioning.

**Why replication factor 3?** With 3 brokers, replication factor 3 means every partition has a copy on all three brokers. The cluster can lose one broker and continue operating with full data integrity. `min.insync.replicas=2` means writes are acknowledged only after 2 replicas confirm — so even during a broker restart, no acknowledged write is lost.

### Partition key

The Fluent Bit Kafka output uses `Message_Key_Field: source_file`. The `source_file` field contains the container's log file path (e.g. `/var/lib/docker/containers/abc123.../abc123...-json.log`). Kafka hashes this key to select a partition. Records from the same container always land on the same partition, preserving per-container ordering. Records from the HTTP and forward inputs have no `source_file` and get round-robin partitioned.

---

## 6. Kafka Connect — moving data into Elasticsearch

Kafka Connect is a framework for running connectors — pluggable components that move data between Kafka and external systems. This stack uses the **Confluent Elasticsearch Sink Connector** (v14.1.1) to read from Kafka topics and index into Elasticsearch.

### Connector installation

The connector is not pre-bundled in the `cp-kafka-connect` image. It is installed at container start via `confluent-hub install`:

```bash
confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:14.1.1
```

The plugins directory is a named Docker volume (`connect-plugins`). The install only runs if the directory doesn't already exist, so subsequent restarts skip it.

### Connector configuration (`elasticsearch-sink.json`)

```json
{
  "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
  "topics": "logs,flog-logs",
  "connection.url": "http://es01:9200,http://es02:9200,http://es03:9200",
  "connection.username": "elastic",
  "connection.password": "changeme",
  "write.method": "insert",
  "data.stream.type": "logs",
  "data.stream.dataset": "fluentbit",
  "data.stream.timestamp.field": "@timestamp"
}
```

**`connection.url`**: multiple URLs for fault tolerance. The connector will try each if one is unavailable. Note this is plain HTTP — the ES cluster uses TLS only for inter-node transport (port 9300), not for the REST API (port 9200).

**`write.method: insert`**: each Kafka record becomes a new document. There is no upsert or deduplication. This is correct for append-only log data.

**Data stream mode**: the connector creates Elasticsearch data streams (not regular indices). A data stream is an abstraction over a series of time-partitioned backing indices. Data streams:
- Are append-only (no updates, no deletes on individual documents)
- Support automatic rollover (a new backing index is created when the current one reaches a size or age threshold)
- Are the preferred storage model for time-series data in Elasticsearch 8.x

**Data stream naming in v14.x**: the connector builds the data stream name as:
```
{data.stream.type}-{data.stream.dataset}-{topic_name}
```

So:
- Topic `logs` → data stream `logs-fluentbit-logs`
- Topic `flog-logs` → data stream `logs-fluentbit-flog-logs`

Note: `data.stream.namespace` is **not** used in v14.x — the topic name is the third segment. This is a connector-version-specific behaviour worth knowing when adding more topics.

### Converters

```json
"key.converter": "org.apache.kafka.connect.storage.StringConverter",
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "false"
```

Kafka messages are raw bytes. Converters tell Connect how to deserialize them:
- **Key**: string (the partition key from Fluent Bit's `source_file` field)
- **Value**: JSON without an embedded schema. `schemas.enable: false` because Fluent Bit writes plain JSON, not Confluent Schema Registry JSON.

### Fault tolerance settings

```json
"flush.timeout.ms": "20000",
"read.timeout.ms": "30000",
"batch.size": "2000"
```

- **`read.timeout.ms: 30000`**: how long the HTTP client waits for a response from Elasticsearch on a bulk request. The default 3000ms is too short when ES is under startup load (security index creation, shard allocation). 30 seconds gives ES time to respond under pressure.
- **`flush.timeout.ms: 20000`**: how long Connect waits to flush a batch before declaring failure.
- **`batch.size: 2000`**: records are buffered and sent to ES in batches of up to 2000, which is far more efficient than one request per record.

### Registering the connector

The connector must be registered via the Connect REST API after the stack starts:

```bash
./scripts/register-connector.sh
```

The script POSTs to `http://localhost:8083/connectors` on first run. On subsequent runs it PUTs to `http://localhost:8083/connectors/logs-es-sink/config` (idempotent update). Connect then starts tasks that consume from Kafka and write to ES.

---

## 7. Elasticsearch — storage and search

### 3-node cluster

The cluster runs three nodes (`es01`, `es02`, `es03`), all configured as master-eligible and data nodes:

```yaml
cluster.name: obs-cluster
cluster.initial_master_nodes: es01,es02,es03
```

`cluster.initial_master_nodes` is only used on the **very first startup** of a fresh cluster. It tells ES which nodes may participate in the initial master election. On all subsequent starts, the elected master is stored in the cluster state on disk — this setting is ignored.

With three master-eligible nodes, the cluster can survive the loss of one node and still elect a master (quorum = 2). If two nodes go down simultaneously, the cluster enters a "no master" state and stops accepting writes (protects against split-brain).

### Why three nodes for a dev stack?

- Exercises real cluster behaviour: master election, shard allocation across nodes, primary/replica placement
- Replication factor 1 (primary + 1 replica) keeps the cluster green even when one node restarts
- Demonstrates how the `elasticsearch` DNS alias routes requests across nodes

### Security architecture

#### Transport TLS (inter-node)

ES 8.x enforces transport TLS when:
- `xpack.security.enabled: true`
- Any node is bound to a non-loopback address

In Docker, all nodes are on `172.x.x.x` addresses — non-loopback — so TLS is mandatory. This applies to the internal port 9300 (cluster gossip, shard replication, master election). The bootstrap check fails with exit code 78 if this condition is not met.

**Certificate generation**: a one-shot `setup` service runs `elasticsearch-certutil` before the ES nodes start:

```bash
elasticsearch-certutil ca --silent --pem -out /certs/ca.zip        # generates CA
elasticsearch-certutil cert --in instances.yml -out /certs/certs.zip  # generates per-node certs
```

`instances.yml` declares the DNS names each node certificate must be valid for:
```yaml
instances:
  - name: es01
    dns: [es01, localhost]
```

The `setup` service mounts a named volume (`certs`). Each ES node also mounts this volume read-only. On second run, the `setup` service finds the CA already present and exits immediately — cert regeneration only happens once per volume lifecycle.

#### HTTP layer (no TLS)

```yaml
xpack.security.http.ssl.enabled: "false"
```

The REST API (port 9200) stays plain HTTP. This keeps all downstream services (Kibana, Kafka Connect, Grafana, curl) simple — they connect to `http://es01:9200` without certificates. Combined with `BIND=127.0.0.1`, this is an acceptable trade-off for a dev environment.

#### Authentication

`ELASTIC_PASSWORD` is picked up by the ES Docker image on first startup of a new cluster to set the `elastic` superuser's password. On subsequent starts with an existing data volume, it has no effect (the password is stored in the `.security-7` index). If you wipe the ES volumes, the password resets to whatever `ELASTIC_PASSWORD` is set to in `.env`.

### Data streams

The connector creates data streams automatically. Each data stream is backed by a sequence of indices:

```
.ds-logs-fluentbit-logs-2024.01.01-000001
.ds-logs-fluentbit-logs-2024.01.15-000002   ← after rollover
```

You query the data stream by name (`logs-fluentbit-*`) and Elasticsearch routes the query to all backing indices transparently. Rollover policy (size, age) can be configured via ILM (Index Lifecycle Management) — not yet implemented in this stack.

### Heap sizing

```yaml
ES_JAVA_OPTS: -Xms${ES_HEAP} -Xmx${ES_HEAP}   # default: 1g
bootstrap.memory_lock: "true"
```

`ES_HEAP=1g` means each of the three nodes gets a 1 GB JVM heap. Total ES heap usage: ~3 GB. Setting `-Xms` and `-Xmx` to the same value prevents JVM heap resizing at runtime (avoids GC pressure from heap growth). `bootstrap.memory_lock: true` mlocks the heap into RAM, preventing the OS from swapping it to disk (swapping an ES node causes severe latency spikes).

---

## 8. Kibana — visualization and detection

### Service accounts: why `kibana_system`, not `elastic`

Kibana needs to write to its own system indices (`.kibana`, `.kibana-task-manager`, `.alerts-security.alerts-*`, etc.). Elasticsearch 8.x blocks the `elastic` superuser from writing to system indices through the Kibana config validation — it rejects startup with a config error. The `kibana_system` built-in user has exactly the privileges Kibana needs and nothing more.

The password for `kibana_system` must be set via the ES API (it doesn't have a default password):

```bash
curl -X POST -u elastic:changeme http://localhost:9200/_security/user/kibana_system/_password \
  -H 'Content-Type: application/json' \
  -d '{"password":"changeme"}'
```

This separates concerns: `elastic` is your admin/browser login; `kibana_system` is the internal service account.

### Encryption key for saved objects

```yaml
XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY: <48-char-hex>
```

Kibana encrypts sensitive fields in saved objects (API keys embedded in detection rule actions, connector credentials, etc.) using this key. Without a persistent key:
- A new random key is generated on every Kibana startup
- Any saved object encrypted with the old key becomes unreadable
- Detection rules become uneditable/undeletable after a restart

The key must be ≥32 characters. It is stored in `.env` and passed via environment variable — never commit the production value to git.

### Elastic Security detection rules

Detection rules live in the Security app (`/app/security/rules`). For this stack, only **Custom Query** rules are available on the free basic licence. Custom Query rules:
- Run on a schedule (e.g. every 1 minute)
- Execute a KQL or Lucene query against an index pattern
- Create alert documents in `.alerts-security.alerts-default` when the query returns results

The detection engine requires `xpack.security.enabled: true` to perform privilege checks — this was the root reason for enabling security.

### Auditd detection rules

Eight detection rules are registered via `scripts/create-detection-rules.sh`, which uses the Kibana Detection Engine REST API (`PUT /api/detection_engine/rules`). The script is idempotent: it checks whether each `rule_id` already exists and uses `POST` (create) or `PUT` (update) accordingly.

All rules target `index: ["logs-fluentbit-*"]` and run on a 1-minute schedule with a 65-second lookback window. The 5-second overlap prevents gaps caused by records that arrive slightly late.

| Rule name | KQL query | Severity | MITRE |
|---|---|---|---|
| Failed Authentication | `audit_type: "USER_AUTH" and message: *res=failed*` | Medium | T1110 Brute Force |
| Direct Root Login | `audit_type: "USER_LOGIN" and message: *uid=0*` | High | T1078 Valid Accounts |
| Sudo Command Executed | `audit_type: "USER_CMD"` | Low | T1548.003 Sudo |
| New User Account Created | `audit_type: "ADD_USER"` | Medium | T1136.001 Local Account |
| User Account Deleted | `audit_type: "DEL_USER"` | High | T1531 Account Removal |
| Audit Config Tampered | `message: *key=auditconfig* or message: *key=audispconfig*` | High | T1562.001 Disable Tools |
| Audit Tools Accessed | `message: *key=audittools*` | Medium | T1082 System Discovery |
| Audit Daemon Stopped | `audit_type: "DAEMON_END"` | Critical | T1562.001 Disable Tools |

**How the `key=` fields work**: the audit rules loaded on this host (`auditctl -l`) include filesystem watches that attach a `key` label to matching events. For example, `-w /etc/audit -p wa -k auditconfig` means any write or attribute-change on `/etc/audit` generates an audit record with `key="auditconfig"` appended to the `message` field. The detection rules match on that suffix, which is more reliable than matching on path names.

**Correlating multi-record events**: a single `execve` syscall produces at minimum a `SYSCALL` + `EXECVE` + `PATH` + `CWD` record, all with the same `audit_serial`. In Kibana Discover, filter by `audit_serial: <value>` to see the full event context for any alert.

---

## 9. Prometheus and Redis Exporter — metrics pipeline

### Why a separate metrics pipeline?

Logs tell you *what happened*. Metrics tell you *how the system is behaving over time*. The two pipelines answer different questions:

- Log pipeline: "show me all error logs from the last hour"
- Metrics pipeline: "what was the Redis ops/sec trend over the last 24 hours?"

### How Prometheus works

Prometheus uses a **pull model**: it scrapes HTTP endpoints (`/metrics`) on a schedule. Each scrape collects a snapshot of metric values in Prometheus text format:

```
redis_commands_processed_total{cmd="get"} 1234
redis_connected_clients 5
```

Prometheus stores these as time series: `(metric_name, labels) → [(timestamp, value), ...]`.

The scrape configuration in `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:9090']    # Prometheus scrapes itself

  - job_name: redis
    static_configs:
      - targets: ['redis-exporter:9121']
```

Retention is 7 days (`--storage.tsdb.retention.time=7d`). Data older than 7 days is automatically deleted.

### Redis Exporter

Redis does not natively expose a Prometheus `/metrics` endpoint. `redis_exporter` bridges this gap: it connects to Redis, runs `INFO` commands, and translates the output into Prometheus format. Key metrics it exposes:

| Metric | Meaning |
|---|---|
| `redis_commands_processed_total` | cumulative command count (rate gives ops/sec) |
| `redis_connected_clients` | current client connections |
| `redis_memory_used_bytes` | heap allocated by Redis |
| `redis_keyspace_hits_total` | cache hit count |
| `redis_keyspace_misses_total` | cache miss count |
| `up{job="redis"}` | 1 if redis-exporter can reach Redis, 0 if not |

### PromQL basics

Grafana queries Prometheus using PromQL. Key patterns:

- **Instant value**: `redis_connected_clients` — current gauge value
- **Rate over time**: `rate(redis_commands_processed_total[1m])` — per-second rate averaged over 1 minute
- **Sum across labels**: `sum(rate(redis_commands_processed_total[1m]))` — total ops/sec across all commands

---

## 10. Grafana — unified dashboards

Grafana is the single pane of glass for both pipelines. It has two provisioned datasources:

### Elasticsearch datasource

```yaml
type: elasticsearch
url: http://es01:9200
basicAuth: true
basicAuthUser: elastic
secureJsonData:
  basicAuthPassword: ${ELASTIC_PASSWORD}
jsonData:
  index: logs-fluentbit-*
  timeField: "@timestamp"
```

Uses the `elastic` superuser (not `kibana_system` — Grafana is not Kibana and has no restriction on which ES user it uses). Queries run against the `logs-fluentbit-*` wildcard, which covers both data streams.

### Prometheus datasource

```yaml
type: prometheus
url: http://prometheus:9090
```

No authentication — Prometheus has no auth in this stack.

### Dashboard provisioning

Dashboards are provisioned from JSON files mounted into the container:

```
grafana/provisioning/dashboards/
├── dashboards.yml         ← tells Grafana where to find dashboard JSON files
├── logs-overview.json     ← Elasticsearch-backed log overview
├── redis-overview.json    ← Prometheus-backed Redis metrics
└── attack-overview.json   ← attack signal dashboard (requires --profile attack)
```

Provisioned dashboards appear automatically on Grafana start. They can be edited in the UI but saving requires writing back to the JSON file (or disabling the provisioning lock).

---

## 11. Redis — the observed service

Redis is the first fully-observed service in the stack. It appears in both pipelines:

- **Logs**: Redis container logs are tailed by Fluent Bit → topic `logs` → data stream `logs-fluentbit-logs`
- **Metrics**: redis-exporter scrapes Redis → Prometheus → Grafana

Redis is configured for ephemeral operation:
```
command: redis-server --save "" --appendonly no
```

`--save ""` disables RDB snapshots. `--appendonly no` disables the AOF log. Redis holds data only in memory — a restart wipes all keys. This is intentional for a dev/demo environment where Redis is the observed target, not the data store.

---

## 12. Attack simulation profile

Start with `docker compose --profile attack up -d`. Three containers run:

### log-injector

Continuously POSTs crafted JSON to Fluent Bit's HTTP input (`http://fluent-bit:8888`):

| Payload | Field used | Detection target |
|---|---|---|
| `{"host":"kibana","msg":"User admin logged in..."}` | `host` | Host impersonation (no `container_name` in injected records) |
| `{"host":"es01","event":"auth.success","user":"root"}` | `event` + `user` | Privileged auth event |
| `{"msg":"<script>alert(1)</script>"}` | `msg` + `ua` | XSS in log fields |
| `{"query":"SELECT * FROM users WHERE name='' OR 1=1--"}` | `query` | SQL injection pattern |
| `{"blob":"<200KB base64>"}` | `blob` | Oversized field (stress test) |

These records land in the `logs` topic → `logs-fluentbit-logs` data stream, queryable in Kibana.

### redis-exploiter

Runs a loop of destructive Redis commands against the `redis` container:

- `KEYS *` — blocking full keyspace scan (DoS risk on large databases)
- `FLUSHALL` — deletes all keys in all databases
- `CONFIG SET dir /tmp` + `CONFIG SET dbfilename evil.sh` + `BGSAVE` — classic unauthenticated RDB file write primitive for persistence/RCE
- `DEBUG SLEEP` — intentionally blocked by Redis's default config
- `REPLICAOF` — attempts to promote itself as a replica

The exploiter's own container logs (stdout) are tailed by Fluent Bit and end up in Elasticsearch with `container_name: redis-exploiter`.

### recon

Runs `nmap -sV` across all services on the `obs` network. The `-sV` flag enables service version detection, which sends probe packets in multiple protocols (HTTP GET, TLS ClientHello, Redis HELP, etc.) to identify what's listening. This is why Kafka brokers log `InvalidReceiveException` with sizes like `0x47455420` ("GET ") — nmap's HTTP probe hit a Kafka port.

---

## 13. Security model

| Layer | Status | Details |
|---|---|---|
| ES inter-node transport | TLS | Self-signed certs via `elasticsearch-certutil`, CA on shared volume |
| ES REST API (HTTP) | Plain HTTP | No TLS — mitigated by `BIND=127.0.0.1` |
| ES authentication | Enabled | `elastic` superuser + `kibana_system` service account |
| Kibana → ES | Basic auth | Uses `kibana_system` with dedicated password |
| Kafka Connect → ES | Basic auth | Uses `elastic` credentials in connector config |
| Grafana → ES | Basic auth | Uses `elastic` credentials in datasource provisioning |
| Kafka | No auth | PLAINTEXT on all listeners |
| Grafana | No auth | Default `admin`/`admin` |
| Prometheus | No auth | No built-in auth |
| Redis | No auth | Unauthenticated by design (the target for redis-exploiter) |
| Host exposure | None | All ports bound to `127.0.0.1` |

---

## 14. Startup order and health checks

Docker Compose `depends_on` with `condition` enforces startup order:

```
setup (cert generation)
    └── es01, es02, es03 (wait for setup to complete)
            ├── kibana (waits for all 3 ES nodes healthy)
            ├── grafana (waits for all 3 ES nodes healthy)
            └── kafka-connect (waits for ES + all 3 Kafka brokers healthy)

kafka1, kafka2, kafka3 (independent, parallel startup)
    ├── kafka-ui (waits for all 3 brokers healthy)
    ├── fluent-bit (waits for all 3 brokers healthy)
    └── kafka-connect (waits for all 3 brokers + ES)
```

Health check definitions:

| Service | Health check |
|---|---|
| `es01/02/03` | `curl -u elastic:$ELASTIC_PASSWORD http://localhost:9200/_cluster/health` and grep for `yellow` or `green` |
| `kafka1/2/3` | `kafka-broker-api-versions --bootstrap-server localhost:9092` |
| `kafka-connect` | `curl http://localhost:8083/` |

ES health `yellow` is accepted (not only `green`) because during fresh cluster startup with empty data streams, the cluster can be yellow briefly while shards are being allocated.

---

## 15. Running the stack

### First-time startup

```bash
# 1. Start all core services
docker compose up -d

# 2. Wait for ES to be green (~60s on first boot due to security index creation)
until curl -sf -u elastic:changeme http://localhost:9200/_cluster/health \
  | python3 -c "import sys,json; s=json.load(sys.stdin)['status']; print(s); exit(0 if s in ['green','yellow'] else 1)"; \
  do sleep 5; done

# 3. Register the Kafka Connect ES sink
./scripts/register-connector.sh

# 4. Import Kibana saved objects (data view, dashboards, saved searches)
./scripts/import-kibana-objects.sh

# 5. Register Kibana Security detection rules (auditd + attack surface)
./scripts/create-detection-rules.sh

# 6. Optional: generate synthetic demo traffic
docker compose --profile demo up -d log-generator redis-benchmark

# 7. Optional: start attack simulators
docker compose --profile attack up -d log-injector redis-exploiter recon
```

### Service URLs

| Service | URL | Credentials |
|---|---|---|
| Kibana | http://localhost:5601 | elastic / changeme |
| Grafana | http://localhost:3000 | admin / admin |
| Kafka UI | http://localhost:8080 | — |
| Elasticsearch | http://localhost:9200 | elastic / changeme |
| Kafka Connect | http://localhost:8083 | — |
| Prometheus | http://localhost:9090 | — |
| Fluent Bit metrics | http://localhost:2020 | — |

### Subsequent restarts

```bash
docker compose up -d
./scripts/register-connector.sh   # idempotent — safe to re-run
```

The `setup` container will start, find the certs already present, and exit immediately. ES nodes start with existing data volumes. Kibana reconnects with the stored encryption key.

### Wiping and starting fresh

```bash
docker compose down
docker volume rm obsdocker_es01-data obsdocker_es02-data obsdocker_es03-data obsdocker_certs
# Then follow first-time startup steps
```

Wiping ES volumes also wipes the `.security-7` index, so the `elastic` password is re-set from `ELASTIC_PASSWORD` on next boot. The `kibana_system` password must be set again manually.

---

## 16. Troubleshooting reference

### Fluent Bit: `proc_records` is 0 on the Kafka output

The Kafka output's tag pattern does not match the records. Check:
- `Match_Regex` on the main output: should be `^(docker\..*|host\..*|app\.forward|app\.http)$`
- `Match` on the flog output: should be `app.flog`
- If you added a new input, what tag does it produce? Does it match one of the patterns?
- Confirm the input is producing records: check `input.records` at `http://localhost:2020/api/v1/metrics`

### Fluent Bit forward input: `flog-logs` topic frozen at 0

`log-generator` uses `fluentd-async=true` on its Docker log driver. If Fluent Bit restarts, the async producer buffers in memory and may silently fail to reconnect. Fix:

```bash
docker compose --profile demo restart log-generator
```

### Kafka Connect: `SocketTimeoutException` on bulk request

ES is under load and bulk responses exceed `read.timeout.ms`. Check:
1. ES cluster health: `curl -u elastic:changeme http://localhost:9200/_cluster/health?pretty`
2. Host load: `docker stats --no-stream`
3. Restart failed tasks: `curl -X POST http://localhost:8083/connectors/logs-es-sink/tasks/0/restart`

If the cluster is green and load is normal, increasing `read.timeout.ms` in `elasticsearch-sink.json` and re-registering fixes it.

### Elasticsearch: exit code 78

Bootstrap check failed. Causes:
- `xpack.security.enabled: true` but `xpack.security.transport.ssl.enabled` not `true` → transport TLS is required on non-loopback interfaces
- `vm.max_map_count` too low on the host → run `sudo sysctl -w vm.max_map_count=262144`

### Kibana: "config validation of [elasticsearch].username: value of 'elastic' is forbidden"

You used the `elastic` superuser as the Kibana service account. Switch to `kibana_system`:
1. Set its password: `curl -X POST -u elastic:changeme http://localhost:9200/_security/user/kibana_system/_password -H 'Content-Type: application/json' -d '{"password":"changeme"}'`
2. Update compose: `ELASTICSEARCH_USERNAME: kibana_system`
3. Restart Kibana: `docker compose up -d kibana`

### Auditd: no records in Elasticsearch

1. Confirm auditd is running: `systemctl is-active auditd`
2. Confirm the log file exists and has content: `sudo ls -lh /var/log/audit/audit.log`
3. Confirm Fluent Bit can read it: `docker exec fluent-bit ls -la /var/log/audit/`
4. Check Fluent Bit picked up the file: look for `inotify_fs_add(): ... name=/var/log/audit/audit.log` in `docker logs fluent-bit`
5. Check the tail input record count at `http://localhost:2020/api/v1/metrics` — the audit tail is `tail.2` (third tail input registered; Docker=`tail.0`, auditd=`tail.2` with `systemd` in between)
6. If records are 0, confirm the volume mount is present: `docker inspect fluent-bit --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}'`

### Auditd: `permission denied` reading log file

The `/var/log/audit` directory is `rwxr-x--- root:adm`. The Fluent Bit container runs as root (uid 0) and should be able to read it. If you see permission errors, check whether the container's user was changed:

```bash
docker inspect fluent-bit --format '{{.Config.User}}'
```

An empty string means root. If a non-root user was set, either revert it or add the container user to the `adm` group on the host.

### Detection engine: "permissions required"

Cause: `xpack.security.enabled: false` on Elasticsearch. The detection engine requires security to be enabled to validate user privileges. Enable security (see security architecture section above).

### Kafka: `InvalidReceiveException: size = 0x47455420 ("GET ")`

This is harmless. nmap's `-sV` probe is sending an HTTP GET to a Kafka broker port. Kafka closes the connection and logs the error. Only appears when `--profile attack` is running.
