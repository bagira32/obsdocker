# obsdocker

Dockerized observability stack:
- **Logs:** Fluent Bit → Kafka (3-broker, KRaft) → Kafka Connect → Elasticsearch → Kibana / Grafana
- **Metrics:** redis-exporter → Prometheus → Grafana

For design rationale, decisions, and tracking, see [CLAUDE.md](CLAUDE.md).

## Quick start

```bash
# 1. Bring everything up (without the demo log generator)
docker compose up -d

# 2. Wait for kafka-connect to be healthy, then register the ES sink
./scripts/register-connector.sh

# 3. (optional, one-time) load the Kibana data view, attack saved searches,
#    and "Attack overview" dashboard
./scripts/import-kibana-objects.sh

# 4. (optional) start demo workloads to produce traffic
docker compose --profile demo up -d log-generator redis-benchmark

# 5. (optional) start the attack simulators — see § Attack simulators
docker compose --profile attack up -d log-injector redis-exploiter recon
```

## Endpoints (all bound to 127.0.0.1)

| Service        | URL                       | Purpose                          |
|----------------|---------------------------|----------------------------------|
| Kibana         | http://localhost:5601     | Logs UI, dev console             |
| Grafana        | http://localhost:3000     | Dashboards (admin/admin)         |
| Elasticsearch  | http://localhost:9200     | ES REST API                      |
| Kafka Connect  | http://localhost:8083     | Connector REST API               |
| Kafka UI       | http://localhost:8080     | Topics, consumer groups          |
| Fluent Bit HTTP| http://localhost:8888     | Push logs in by HTTP             |
| Fluent Bit Fwd | localhost:24224 (tcp/udp) | Fluent Forward protocol          |
| Fluent Bit ops | http://localhost:2020     | Fluent Bit metrics endpoint      |
| Kafka external | localhost:19092..19094    | Bootstrap from host              |
| Prometheus     | http://localhost:9090     | PromQL UI, target status         |
| Redis exporter | http://localhost:9121     | `/metrics` for Prometheus        |
| Redis          | localhost:6379            | `redis-cli -h localhost`         |

## Where to find logs

Fluent Bit routes by tag into two Kafka topics; Kafka Connect then mirrors each into its own Elasticsearch data stream.

| Source                                              | Fluent Bit tag    | Kafka topic | ES data stream                |
|-----------------------------------------------------|-------------------|-------------|--------------------------------|
| Docker container JSON logs (`/var/lib/docker/...`)  | `docker.*`        | `logs`      | `logs-fluentbit-logs`         |
| Host systemd journal                                | `host.systemd`    | `logs`      | `logs-fluentbit-logs`         |
| HTTP push at `:8888`                                | `app.http`        | `logs`      | `logs-fluentbit-logs`         |
| Fluent Forward at `:24224` (ad-hoc clients)         | (client-supplied) | `logs`      | `logs-fluentbit-logs`         |
| `log-generator` via docker fluentd driver           | `app.flog`        | `flog-logs` | `logs-fluentbit-flog-logs`    |

A single wildcard — `logs-fluentbit-*` — selects everything in Grafana / Kibana / ES.

### In Kafka

```bash
# List topics
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list

# Tail the main logs topic (Ctrl-C to stop)
docker exec -it kafka1 kafka-console-consumer \
     --bootstrap-server localhost:9092 --topic logs

# Tail the synthetic-generator topic
docker exec -it kafka1 kafka-console-consumer \
     --bootstrap-server localhost:9092 --topic flog-logs

# Per-partition end offsets (how many messages per partition)
docker exec kafka1 kafka-get-offsets \
     --bootstrap-server localhost:9092 --topic logs --time -1

# Sink consumer-group lag
docker exec kafka1 kafka-consumer-groups \
     --bootstrap-server localhost:9092 --describe --group connect-logs-es-sink
```

Or open **Kafka UI** at http://localhost:8080 → cluster `obs` → Topics → `logs` / `flog-logs` → Messages.

### In Elasticsearch

```bash
# All data streams matching our naming
curl -s 'http://localhost:9200/_data_stream/logs-fluentbit-*?pretty'

# Underlying backing indices and doc counts
curl -s 'http://localhost:9200/_cat/indices/logs-fluentbit-*?v'

# Doc count per stream
curl -s 'http://localhost:9200/logs-fluentbit-logs/_count?pretty'
curl -s 'http://localhost:9200/logs-fluentbit-flog-logs/_count?pretty'

# Newest 3 docs across both streams
curl -s 'http://localhost:9200/logs-fluentbit-*/_search?pretty' \
     -H 'Content-Type: application/json' -d '{
       "size": 3,
       "sort": [{"@timestamp":"desc"}],
       "_source": ["@timestamp","container_name","source_file","log","level"]
     }'

# Newest 3 docs only from the synthetic generator
curl -s 'http://localhost:9200/logs-fluentbit-flog-logs/_search?pretty' \
     -H 'Content-Type: application/json' -d '{
       "size": 3, "sort": [{"@timestamp":"desc"}]
     }'
```

In **Kibana** (http://localhost:5601): Stack Management → Data Views → Create with index pattern `logs-fluentbit-*` and timestamp field `@timestamp`, then go to Discover. In **Grafana** (http://localhost:3000, admin/admin): the `Elasticsearch` datasource is pre-provisioned against the same wildcard; the **Logs overview** dashboard is the default landing.

## Where to find metrics

`redis-exporter` scrapes Redis via `INFO` and exposes a Prometheus `/metrics` endpoint; Prometheus scrapes it every 15s and retains samples for 7 days. Redis container logs continue to flow through the standard Fluent Bit tail → `logs` topic path, so the same instance shows up in both pipelines.

```bash
# Prometheus target health (expect both targets UP)
curl -s 'http://localhost:9090/api/v1/targets?state=active' | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'

# Instant query: current Redis memory in bytes
curl -s --data-urlencode 'query=redis_memory_used_bytes' \
     http://localhost:9090/api/v1/query | jq '.data.result'

# Instant query: ops/sec over the last minute
curl -s --data-urlencode 'query=rate(redis_commands_processed_total[1m])' \
     http://localhost:9090/api/v1/query | jq '.data.result'

# Raw exporter output
curl -s http://localhost:9121/metrics | grep -E '^redis_(memory_used_bytes|connected_clients|commands_processed_total) '
```

In **Grafana**: the `Prometheus` datasource is pre-provisioned (UID `prometheus`), and the **Redis overview** dashboard (memory, clients, hit ratio, ops/sec, keyspace hits/misses, network, evictions/expirations) is provisioned alongside `Logs overview`.

## Attack simulators

The stack runs with **no auth and no TLS** (mitigated only by `BIND=127.0.0.1`), so it's a natural sandbox for poking at attacker behaviors. Three bad-actor containers live behind the `attack` compose profile.

> **Heads up:** the Redis exploiter is **destructive** — it issues `FLUSHALL`, `CONFIG SET dir/dbfilename`, `BGSAVE`, and `REPLICAOF` against the local `redis` service every iteration. The redis container is ephemeral (`--save "" --appendonly no`), so this is safe on the dev box but anything `redis-benchmark` (demo profile) generated will be wiped repeatedly while the exploiter runs.

### Bring them up

Prereqs: the core stack is up (`docker compose up -d`), the ES sink is registered (`./scripts/register-connector.sh`), and the Kibana objects are imported (`./scripts/import-kibana-objects.sh`).

```bash
# Start all three simulators
docker compose --profile attack up -d log-injector redis-exploiter recon

# Or start just one
docker compose --profile attack up -d log-injector
docker compose --profile attack up -d redis-exploiter
docker compose --profile attack up -d recon
```

### What each container does

| Container         | Behavior                                                                                                                                                                                       | Lands as                                                                                  |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| `log-injector`    | POSTs crafted JSON to `fluent-bit:8888` every 5s: spoofed `host` (kibana/es01/app), fake `event:auth.success`, XSS in `msg`/`ua`, SQLi-shaped `query`, oversized `blob` (200KB), malformed JSON | New records in `logs-fluentbit-logs` with attacker-only fields                            |
| `redis-exploiter` | Drives the unauthenticated-Redis playbook against `redis:6379`: recon (`INFO`, `CLIENT LIST`), keyspace pollution (2000 pipelined `SET`s), `KEYS *`, RDB-write precursor, refused `DEBUG`, `REPLICAOF` to a bogus master, `FLUSHALL` | Grafana Redis metrics spike + Redis container log lines (BGSAVE / REPLICAOF / replication state) |
| `recon`           | `nmap -sV` service-version sweep + NSE HTTP discovery (`http-title`, `http-headers`, `http-enum`, `http-methods`, `http-robots.txt`) against every named service on the `obs` network          | Dense connection patterns + Nmap NSE User-Agent strings + discovery paths in target logs   |

### Where to see the signals

- **Grafana** → "Attack overview" dashboard (auto-provisioned at [http://localhost:3000/dashboards](http://localhost:3000/dashboards), UID `obs-attack-overview`): injected events by spoofed host, recon impact by container, Redis ops/sec + key count (sawtooth), suspicious-payload feed.
- **Kibana** → "Attack overview" dashboard ([http://localhost:5601/app/dashboards#/view/obs-attack-dashboard](http://localhost:5601/app/dashboards#/view/obs-attack-dashboard)): five Discover-based panels for forensic drill-in on spoofed hosts, injected payloads, oversized blobs, Redis abuse, and network recon.

Quick CLI sanity checks while they run:

```bash
# Injected events arriving (expect a non-zero count after ~30s)
curl -s 'http://localhost:9200/logs-fluentbit-logs/_count?pretty' \
     -H 'Content-Type: application/json' -d '{
       "query": {"query_string": {"query": "host:(kibana OR es01 OR app) OR _exists_:blob OR _exists_:ua"}}
     }'

# Redis ops/sec spiking under the exploiter's pipelined SETs
curl -s --data-urlencode 'query=rate(redis_commands_processed_total[1m])' \
     http://localhost:9090/api/v1/query | jq '.data.result[0].value[1]'

# Recon lines (Nmap NSE UA)
curl -s 'http://localhost:9200/logs-fluentbit-logs/_count?pretty' \
     -H 'Content-Type: application/json' -d '{
       "query": {"match_phrase": {"log": "Nmap Scripting Engine"}}
     }'
```

### Kernel-level tracing (ftrace / bpftrace)

Logs and metrics show the *outcome* of the attacks (records arriving in ES, ops/sec spiking). To see the *kernel-side* of the same activity — syscalls, network connects, process forks — trace from the host. Containers are just cgroup-scoped processes, so both ftrace and bpftrace can target them by **cgroup path**, with no container-aware tooling needed.

Prereqs (Kali): `sudo apt install -y trace-cmd bpftrace`. ftrace requires root.

Helper to resolve a container to its cgroup v2 path:

```bash
container_cgroup() {
  local pid; pid=$(docker inspect -f '{{.State.Pid}}' "$1")
  echo "/sys/fs/cgroup$(awk -F: '/^0::/ {print $3}' /proc/$pid/cgroup)"
}
```

**ftrace via `trace-cmd` (scoped by cgroup).** Capture `connect()` and `sendto()` syscalls from inside the recon container for 20s — every NSE probe and port scan shows up:

```bash
sudo trace-cmd record \
  -G "$(container_cgroup recon)" \
  -e syscalls:sys_enter_connect \
  -e syscalls:sys_enter_sendto \
  sleep 20
sudo trace-cmd report | head -40
```

For the redis-exploiter, swap the event set for one that highlights its fork-per-command pattern:

```bash
sudo trace-cmd record \
  -G "$(container_cgroup redis-exploiter)" \
  -e sched:sched_process_exec \
  -e syscalls:sys_enter_connect \
  sleep 20
sudo trace-cmd report | grep -E 'exec|connect' | head -40
```

**bpftrace one-liners (live, aggregated).** Live count of connection attempts per command name inside the recon container, every 5s:

```bash
sudo bpftrace -e '
tracepoint:syscalls:sys_enter_connect
/cgroup == cgroupid("'"$(container_cgroup recon)"'")/ {
  @conns[comm] = count();
}
interval:s:5 { print(@conns); clear(@conns); }'
```

Catch *which destination IP/port* the redis-exploiter's `REPLICAOF` actually tried (handy because the bogus master attempt only shows up at the syscall layer):

```bash
sudo bpftrace -e '
kprobe:tcp_v4_connect
/cgroup == cgroupid("'"$(container_cgroup redis-exploiter)"'")/ {
  $sk = (struct sock *)arg0;
  printf("%s -> %s:%d\n", comm,
         ntop(($sk->__sk_common.skc_daddr)),
         bswap(($sk->__sk_common.skc_dport)));
}'
```

Watch the log-injector's curl-per-request pattern (every POST is a fresh process + a fresh `connect`):

```bash
sudo bpftrace -e '
tracepoint:sched:sched_process_exec
/cgroup == cgroupid("'"$(container_cgroup log-injector)"'")/ {
  printf("%s exec %s\n", strftime("%H:%M:%S", nsecs), str(args->filename));
}'
```

Caveats: ftrace `syscalls/*` enabled wholesale is a firehose — keep the cgroup filter on. bpftrace's `cgroupid("...")` only resolves at script load, so if you restart a container its cgroup ID changes and you need to relaunch the script. Neither tool annotates the trace with the container *name* — you map back via the cgroup path or `comm`/`pid`.

### Tear them down

```bash
# Stop the attackers (keeps the rest of the stack running)
docker compose --profile attack stop log-injector redis-exploiter recon
docker compose --profile attack rm -f log-injector redis-exploiter recon

# Or, if you want the keyspace clean afterwards
docker exec redis redis-cli FLUSHALL
```

## Smoke tests

```bash
# Push a log via Fluent Bit's HTTP input (lands in `logs` topic → `logs-fluentbit-logs`)
MARKER="probe-$(date +%s)"
curl -X POST -H 'Content-Type: application/json' \
     -d "{\"log\":\"$MARKER\",\"level\":\"info\"}" \
     http://localhost:8888/test
echo "marker=$MARKER"

# Find it in Elasticsearch
curl -s "http://localhost:9200/logs-fluentbit-*/_search?pretty" \
     -H 'Content-Type: application/json' -d "{
       \"query\": { \"match_phrase\": { \"log\": \"$MARKER\" } }
     }"

# Drive some Redis traffic and confirm Prometheus sees the bump
docker exec redis redis-cli -r 1000 -i 0 SET probe:$(date +%s) hello >/dev/null
sleep 20  # ≥ one scrape interval
curl -s --data-urlencode 'query=rate(redis_commands_processed_total[1m])' \
     http://localhost:9090/api/v1/query | jq '.data.result[0].value[1]'
```

## Common operations

```bash
# Tail Fluent Bit
docker logs -f fluent-bit

# Inspect connector status
curl -s http://localhost:8083/connectors/logs-es-sink/status | jq

# Inspect Prometheus targets
curl -s 'http://localhost:9090/api/v1/targets?state=active' | jq '.data.activeTargets[] | {job: .labels.job, health: .health, lastError: .lastError}'

# Tail Redis
docker logs -f redis

# Stop everything (keeps volumes)
docker compose down

# Nuke everything including data
docker compose down -v
```
