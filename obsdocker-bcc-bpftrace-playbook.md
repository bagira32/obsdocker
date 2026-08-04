# obsdocker — BCC & bpftrace Performance and Health Check Playbook

**Services:** Fluent Bit · Kafka ×3 · Elasticsearch ×3 · Kafka Connect · Redis · Grafana · Kibana  
**Tools:** bpftrace (primary) · BCC `bpfcc-tools` (kernel < 7.x only)  
**Purpose:** Operational health check and live performance triage — answers "is the pipeline healthy and why is it slow?"

> The existing `obsdocker-performance-playbook.md` covers `perf` and `ftrace` for baseline/attack comparison.  
> This playbook is focused on health checks and live investigation via bpftrace and BCC.

---

## Prerequisites

```bash
# Install bpftrace and BCC tools (Kali/Debian)
sudo apt-get install -y bpftrace bpfcc-tools linux-headers-$(uname -r)

# Verify bpftrace (uses BTF — works on all supported kernels)
sudo bpftrace --version

# Verify BCC (see compatibility note below)
sudo biolatency-bpfcc --help 2>&1 | head -1

# Allow perf events for current user (or prefix all commands with sudo)
sudo sysctl kernel.perf_event_paranoid=1

# Collect container PIDs — export in every shell session using this playbook
FB_PID=$(docker inspect --format '{{.State.Pid}}' fluent-bit)
KAFKA_PID=$(docker inspect --format '{{.State.Pid}}' kafka1)
ES_PID=$(docker inspect --format '{{.State.Pid}}' es01)
KC_PID=$(docker inspect --format '{{.State.Pid}}' kafka-connect)
REDIS_PID=$(docker inspect --format '{{.State.Pid}}' redis)
echo "fb=$FB_PID kafka=$KAFKA_PID es=$ES_PID kc=$KC_PID redis=$REDIS_PID"
```

### Kernel 7.x compatibility

BCC tools (`bpfcc-tools` 0.35.0) **fail to compile on kernel 7.x** due to struct layout changes in `filename` and `ns_common`. All BCC one-liners in this playbook have been replaced with bpftrace equivalents that use BTF and work on kernel 7.x.

| Tool state | Details |
|---|---|
| **bpftrace** | Fully working — uses BTF from `/sys/kernel/btf/vmlinux`, no header compilation |
| **BCC (`*-bpfcc`)** | Broken on kernel ≥ 7.x — static assertion failure at compile time |

If you are on kernel < 7.x and BCC tools work, the original one-liners are noted in comments where they differ from the bpftrace versions.

---

## Quick Triage (< 5 minutes)

Run in order. Together these give a full-stack health picture before drilling deeper.
All commands have fixed windows and exit cleanly.

### OOM kill watcher — start first, leave running in background
```bash
sudo bpftrace -e '
tracepoint:oom:mark_victim {
  printf("OOM KILL: pid=%d comm=%s\n", args->pid, args->comm);
}' &
# Prints immediately on any OOM kill — any output here is critical
```

### Run queue latency (10s window)
```bash
sudo bpftrace -e '
tracepoint:sched:sched_wakeup,
tracepoint:sched:sched_wakeup_new {
  @qstart[args->pid] = nsecs;
}
tracepoint:sched:sched_switch {
  if (@qstart[args->next_pid]) {
    @runqlat_us = hist((nsecs - @qstart[args->next_pid]) / 1000);
    @_d = delete(@qstart[args->next_pid]);
  }
}
interval:s:10 {
  print(@runqlat_us);
  clear(@_d);
  exit();
}' 2>/dev/null
# Healthy: mass in 1–4 µs buckets
# Degraded: significant mass ≥ 1ms → CPU overloaded or scheduler starvation
```

### Block I/O latency (10s window)
```bash
sudo bpftrace -e '
tracepoint:block:block_rq_issue {
  @start[args->dev, args->sector] = nsecs;
}
tracepoint:block:block_rq_complete
/@start[args->dev, args->sector]/ {
  @io_lat_ms = hist((nsecs - @start[args->dev, args->sector]) / 1000000);
  @_d = delete(@start[args->dev, args->sector]);
}
interval:s:10 {
  print(@io_lat_ms);
  clear(@_d);
  exit();
}' 2>/dev/null
# Healthy (SSD): P99 < 5ms
# Degraded: P99 > 50ms → ES segment merge, Kafka log flush, or disk pressure
```

### Page cache hit rate (three 5s samples)
```bash
sudo bpftrace -e '
tracepoint:filemap:mm_filemap_get_pages         { @hits   = count(); }
tracepoint:filemap:mm_filemap_add_to_page_cache { @misses = count(); }
interval:s:5 {
  $h = (int64)@hits; $m = (int64)@misses;
  $total = $h + $m;
  $ratio = $total > 0 ? $h * 100 / $total : 0;
  printf("HITS: %d  MISSES: %d  HITRATIO: %d%%\n", $h, $m, $ratio);
  clear(@hits); clear(@misses);
}
interval:s:15 { exit(); }' 2>/dev/null
# Healthy: HITRATIO > 95% at idle
# Low ratio → ES Lucene segment eviction or Kafka log reads from disk
```

### New TCP connections (15s window)
```bash
sudo bpftrace -e '
tracepoint:sock:inet_sock_set_state
/args->newstate == 1/ {
  @conns[comm] = count();
}
interval:s:15 {
  printf("New TCP connections by process:\n");
  print(@conns);
  exit();
}' 2>/dev/null
# Healthy: only inter-service processes (fluent-bit, kafka, connect, redis-benchmark)
# Flag: unexpected process names → misconfigured client or external scan
```

---

## 1. CPU & Scheduler Health

### 1.1 Run queue latency — system-wide
```bash
sudo bpftrace -e '
tracepoint:sched:sched_wakeup,
tracepoint:sched:sched_wakeup_new {
  @qstart[args->pid] = nsecs;
}
tracepoint:sched:sched_switch {
  if (@qstart[args->next_pid]) {
    @runqlat_us = hist((nsecs - @qstart[args->next_pid]) / 1000);
    @_d = delete(@qstart[args->next_pid]);
  }
}
interval:s:30 {
  print(@runqlat_us);
  clear(@_d);
  exit();
}' 2>/dev/null
# Thresholds: P99 < 1ms healthy, > 10ms critical
```

### 1.2 On-CPU time distribution per container
```bash
# How long do threads hold CPU before yielding?
sudo bpftrace -e '
tracepoint:sched:sched_switch
/args->prev_state == 0/ {
  @oncpu_us[args->prev_comm] = hist(
    (nsecs - @start[args->prev_pid]) / 1000
  );
}
tracepoint:sched:sched_switch {
  @start[args->next_pid] = nsecs;
}
interval:s:30 {
  print(@oncpu_us);
  exit();
}' 2>/dev/null
# Healthy: bimodal — short slices (I/O-bound) + medium slices (compute)
# Flag: long tail in the ms range → GC pause, tight loop, or spinning lock holder
```

### 1.3 Off-CPU time — what is each service blocked on?
```bash
# Requires kernel with frame pointers or DWARF. Shows kernel stacks only (-K flag).
# Fluent Bit
sudo bpftrace -e '
tracepoint:sched:sched_switch
/args->prev_pid == '$FB_PID'/ {
  @offcpu_start = nsecs;
}
tracepoint:sched:sched_switch
/args->next_pid == '$FB_PID' && @offcpu_start > 0/ {
  @offcpu_us = hist((nsecs - @offcpu_start) / 1000);
  @offcpu_start = 0;
}
interval:s:30 {
  print(@offcpu_us);
  exit();
}' 2>/dev/null
```

Interpretation:

| Dominant latency bucket | Meaning |
|---|---|
| < 100 µs | Normal I/O scheduling, expected |
| 1–10 ms | Lock contention (Kafka produce lock, ES merge lock) or disk I/O |
| > 100 ms | GC pause (ES/Kafka JVM), major I/O stall |

### 1.4 Page fault rate per process
```bash
sudo bpftrace -e '
software:page-faults:1 {
  @faults[comm] = count();
}
interval:s:30 {
  print(@faults);
  exit();
}' 2>/dev/null
# ES dominant at startup → Lucene mmap cold segment loading (expected)
# Sustained high rate after warmup → segment churn; consider forcemerge
```

---

## 2. Memory Health

### 2.1 RSS + swap snapshot — all containers
```bash
for name in kafka1 kafka2 kafka3 es01 es02 es03 \
            fluent-bit kafka-connect kibana grafana redis; do
  PID=$(docker inspect --format '{{.State.Pid}}' $name 2>/dev/null)
  [ -z "$PID" ] && continue
  RSS=$(awk  '/^VmRSS/{print $2}'  /proc/$PID/status 2>/dev/null)
  SWAP=$(awk '/^VmSwap/{print $2}' /proc/$PID/status 2>/dev/null)
  echo "$name: RSS=${RSS}kB SWAP=${SWAP:-0}kB"
done
# Flag: any SWAP > 0 on ES or Kafka → JVM heap paged out → severe latency incoming
```

### 2.2 cgroup memory pressure per container
```bash
for name in kafka1 es01 fluent-bit kafka-connect redis; do
  PID=$(docker inspect --format '{{.State.Pid}}' $name 2>/dev/null)
  [ -z "$PID" ] && continue
  CG=$(awk -F: '{print $3}' /proc/$PID/cgroup | head -1)
  BASE="/sys/fs/cgroup${CG}"
  MEM=$(cat $BASE/memory.current 2>/dev/null)
  PRESSURE=$(grep '^some' $BASE/memory.pressure 2>/dev/null | awk '{print $2}')
  OOM=$(awk '/oom_kill/{print $2}' $BASE/memory.events 2>/dev/null)
  echo "$name: MEM=$((MEM/1024/1024))MB pressure=${PRESSURE:-0} OOM=${OOM:-0}"
done
# Flag: OOM > 0 is critical; pressure avg10 > 10% indicates memory contention
```

### 2.3 bpftrace: live RSS snapshot per container
```bash
sudo bpftrace -e '
profile:hz:1
/curtask->mm != 0/ {
  $mm = curtask->mm;
  $pages = $mm->rss_stat[0].count   /* MM_FILEPAGES  */
         + $mm->rss_stat[1].count   /* MM_ANONPAGES  */
         + $mm->rss_stat[2].count;  /* MM_SHMEMPAGES */
  @rss_kb[cgroup, comm] = max($pages * 4);
}
interval:s:10 {
  print(@rss_kb);
  clear(@rss_kb);
}
interval:s:60 {
  exit();
}' 2>/dev/null
# Map cgroup IDs to container names:
# find /sys/fs/cgroup -maxdepth 5 -path "*docker*" -name "*.scope" \
#   | while read d; do echo "$(stat -c %i $d)  $(basename $d)"; done
```

### 2.4 Page cache hit rate (continuous)
```bash
sudo bpftrace -e '
tracepoint:filemap:mm_filemap_get_pages         { @hits   = count(); }
tracepoint:filemap:mm_filemap_add_to_page_cache { @misses = count(); }
interval:s:10 {
  $h = (int64)@hits; $m = (int64)@misses;
  $total = $h + $m;
  $ratio = $total > 0 ? $h * 100 / $total : 0;
  printf("%s  HITS: %d  MISSES: %d  HITRATIO: %d%%\n",
    strftime("%H:%M:%S", nsecs), $h, $m, $ratio);
  clear(@hits); clear(@misses);
}' 2>/dev/null
# Ctrl+C to stop
# Flag: ratio drops below 90% → eviction pressure; ES or Kafka reading cold data
```

### 2.5 OOM kill watcher
```bash
sudo bpftrace -e '
tracepoint:oom:mark_victim {
  printf("OOM KILL: pid=%d comm=%s time=%s\n",
    args->pid, args->comm, strftime("%H:%M:%S", nsecs));
}' 2>/dev/null
# Runs continuously — any output is critical
# Identify which cgroup was killed and correlate via section 2.2
```

### 2.6 Memory leak detection (native allocations only)
```bash
# bpftrace: track kernel slab allocations not freed per process
sudo bpftrace -e '
tracepoint:kmem:kmalloc /pid == '$FB_PID'/ {
  @alloc_bytes = sum(args->bytes_alloc);
  @alloc_count = count();
}
tracepoint:kmem:kfree /pid == '$FB_PID'/ {
  @free_count = count();
}
interval:s:10 {
  printf("allocs=%d  frees=%d  net_bytes=%d\n",
    @alloc_count, @free_count, @alloc_bytes);
  clear(@alloc_bytes); clear(@alloc_count); clear(@free_count);
}
interval:s:60 { exit(); }' 2>/dev/null
# Growing net_bytes with allocs >> frees → native memory leak
# JVM heap leaks (ES, Kafka) are not visible here — use JVM GC logs + heap dumps
```

---

## 3. Network Health

### 3.1 Live TCP throughput per process
```bash
sudo bpftrace -e '
kprobe:tcp_sendmsg { @tx[comm] = sum(arg2); }
kprobe:tcp_recvmsg { @rx[comm] = sum(arg2); }
interval:s:5 {
  printf("\n%s\n", strftime("%H:%M:%S", nsecs));
  printf("-- TX bytes/5s --\n"); print(@tx);
  printf("-- RX bytes/5s --\n"); print(@rx);
  clear(@tx); clear(@rx);
}' 2>/dev/null
# Ctrl+C to stop. Focus on: fluent-bit (TX→Kafka), kafka-connect (TX→ES)
```

### 3.2 TCP session lifetimes
```bash
sudo bpftrace -e '
tracepoint:sock:inet_sock_set_state
/args->newstate == 1/ {
  @conn_start[args->skaddr] = nsecs;
  @conn_comm[args->skaddr]  = comm;
}
tracepoint:sock:inet_sock_set_state
/args->newstate == 7 && @conn_start[args->skaddr]/ {
  $ms = (nsecs - @conn_start[args->skaddr]) / 1000000;
  printf("%-16s  %dms\n", @conn_comm[args->skaddr], $ms);
  delete(@conn_start[args->skaddr]);
  delete(@conn_comm[args->skaddr]);
}' 2>/dev/null
# Ctrl+C to stop
# Healthy: long-lived sessions (MS in millions) on Kafka/ES ports
# Flag: short lifetimes (< 1000ms) on Kafka/ES → reconnect loops
```

### 3.3 New outbound connections (continuous)
```bash
sudo bpftrace -e '
kprobe:tcp_v4_connect {
  printf("CONNECT  pid=%-6d  comm=%-20s\n", pid, comm);
}' 2>/dev/null
# Ctrl+C to stop
# Healthy: only inter-service processes
# Flag: unexpected process or external IP → misconfigured client or data exfil
```

### 3.4 Inbound accepted connections (continuous)
```bash
sudo bpftrace -e '
tracepoint:sock:inet_sock_set_state
/args->newstate == 4/ {
  printf("ACCEPT   pid=%-6d  comm=%-20s  sport=%d\n",
    pid, comm, args->sport);
}' 2>/dev/null
# Ctrl+C to stop
# Flag: rapid burst of short-lived accepts → port scan (nmap recon profile)
```

### 3.5 Per-process TCP throughput (rolling)
```bash
sudo bpftrace -e '
kprobe:tcp_sendmsg { @tx_bytes[comm] = sum(arg2); }
kprobe:tcp_recvmsg { @rx_bytes[comm] = sum(arg2); }
interval:s:10 {
  printf("\n-- TX --\n"); print(@tx_bytes);
  printf("-- RX --\n"); print(@rx_bytes);
  clear(@tx_bytes); clear(@rx_bytes);
}' 2>/dev/null
```

### 3.6 HTTP ingest payload size distribution (fluent-bit :8888)
```bash
sudo bpftrace -e '
kprobe:tcp_recvmsg
/comm == "fluent-bit"/ {
  @payload_bytes = hist(arg2);
}
interval:s:30 {
  print(@payload_bytes);
  exit();
}' 2>/dev/null
# Healthy: most payloads < 4KB (normal log lines)
# Flag: payloads in 64KB–256KB range → oversized blobs (log-injector attack profile)
```

---

## 4. Disk I/O Health

### 4.1 Block I/O latency histogram
```bash
sudo bpftrace -e '
tracepoint:block:block_rq_issue {
  @start[args->dev, args->sector] = nsecs;
}
tracepoint:block:block_rq_complete
/@start[args->dev, args->sector]/ {
  @io_lat_ms = hist((nsecs - @start[args->dev, args->sector]) / 1000000);
  @_d = delete(@start[args->dev, args->sector]);
}
interval:s:30 {
  print(@io_lat_ms);
  clear(@_d);
  exit();
}' 2>/dev/null
# Healthy (SSD): P50 < 0.1ms, P99 < 5ms
# Degraded: P99 > 50ms → ES segment merge, Kafka log flush, or I/O scheduler pressure
```

### 4.2 Top block I/O by process
```bash
sudo bpftrace -e '
tracepoint:block:block_rq_issue {
  @io_count[comm] = count();
  @io_bytes[comm] = sum(args->nr_sector * 512);
}
interval:s:5 {
  printf("\n%s  -- I/O count --\n", strftime("%H:%M:%S", nsecs));
  print(@io_count);
  printf("-- I/O bytes --\n");
  print(@io_bytes);
  clear(@io_count); clear(@io_bytes);
}' 2>/dev/null
# Ctrl+C to stop
# Expected heavy writers: es01/02/03 (segment flush), kafka1/2/3 (log append)
```

### 4.3 File open patterns per service
```bash
# Fluent Bit — should open new container log files as they appear
sudo bpftrace -e '
tracepoint:syscalls:sys_enter_openat
/pid == '$FB_PID'/ {
  printf("%-6d %-16s %s\n", pid, comm, str(args->filename));
}' 2>/dev/null

# Elasticsearch — segment files, translog
sudo bpftrace -e '
tracepoint:syscalls:sys_enter_openat
/pid == '$ES_PID'/ {
  printf("%-6d %-16s %s\n", pid, comm, str(args->filename));
}' 2>/dev/null

# Kafka — .log, .index, .timeindex files
sudo bpftrace -e '
tracepoint:syscalls:sys_enter_openat
/pid == '$KAFKA_PID'/ {
  printf("%-6d %-16s %s\n", pid, comm, str(args->filename));
}' 2>/dev/null
```

### 4.4 Block I/O latency in microseconds (higher resolution)
```bash
sudo bpftrace -e '
tracepoint:block:block_rq_issue {
  @start[args->dev, args->sector] = nsecs;
}
tracepoint:block:block_rq_complete
/@start[args->dev, args->sector]/ {
  @io_lat_us = hist((nsecs - @start[args->dev, args->sector]) / 1000);
  @_d = delete(@start[args->dev, args->sector]);
}
interval:s:30 {
  print(@io_lat_us);
  clear(@_d);
  exit();
}' 2>/dev/null
```

### 4.5 Write size distribution (Kafka vs ES)
```bash
sudo bpftrace -e '
tracepoint:syscalls:sys_exit_write
/(pid == '$KAFKA_PID' || pid == '$ES_PID') && args->ret > 0/ {
  @write_bytes[comm] = hist(args->ret);
}
interval:s:30 {
  print(@write_bytes);
  exit();
}' 2>/dev/null
# Kafka: mostly 64KB–1MB log segment appends
# ES: small translog writes + large segment flush spikes
```

---

## 5. Per-Service Checks

### 5.1 Fluent Bit

```bash
# Plugin-level records in/out + errors per plugin
curl -s http://localhost:2020/api/v1/metrics | python3 -m json.tool

# Overall health
curl -s http://localhost:2020/api/v1/health
```

```bash
# Tail read activity — confirms FB is actively reading log files
sudo bpftrace -e '
tracepoint:syscalls:sys_enter_read
/pid == '$FB_PID'/ {
  @reads = count();
}
interval:s:5 {
  printf("FB read syscalls: %d/5s\n", @reads);
  clear(@reads);
}' 2>/dev/null
# Near-zero → tail input idle (no new logs or stuck inotify)
```

```bash
# Kafka produce batch sizes from FB
sudo bpftrace -e '
kprobe:tcp_sendmsg
/pid == '$FB_PID'/ {
  @batch_bytes = hist(arg2);
}
interval:s:30 {
  print(@batch_bytes);
  exit();
}' 2>/dev/null
```

```bash
# Connection stability — FB should hold one long-lived conn per Kafka broker
sudo bpftrace -e '
tracepoint:sock:inet_sock_set_state
/args->dport == 9092 || args->dport == 9093/ {
  if (args->newstate == 1) {
    printf("CONNECT  comm=%-16s  -> port %d\n", comm, args->dport);
  }
  if (args->newstate == 7) {
    printf("CLOSE    comm=%-16s  port %d\n",    comm, args->dport);
  }
}' 2>/dev/null
# Healthy: 3 CONNECT events at startup, no CLOSE events during normal ops
# Flag: repeated CONNECT+CLOSE cycles → FB reconnecting to Kafka
```

### 5.2 Kafka

```bash
# Consumer group lag — primary pipeline health signal
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --all-groups

# Topic offset growth rate (run twice, 30s apart, diff)
docker exec kafka1 kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --time -1
```

```bash
# Kafka write throughput (log segment appends)
sudo bpftrace -e '
tracepoint:syscalls:sys_exit_write
/pid == '$KAFKA_PID' && args->ret > 0/ {
  @write_bytes = sum(args->ret);
  @write_count = count();
}
interval:s:10 {
  printf("Kafka writes: count=%d  bytes=%d\n", @write_count, @write_bytes);
  clear(@write_bytes); clear(@write_count);
}' 2>/dev/null
```

```bash
# Scheduler latency for Kafka threads
sudo bpftrace -e '
tracepoint:sched:sched_wakeup /args->pid == '$KAFKA_PID'/ {
  @qstart = nsecs;
}
tracepoint:sched:sched_switch /args->next_pid == '$KAFKA_PID' && @qstart/ {
  @kafka_runqlat_us = hist((nsecs - @qstart) / 1000);
  @qstart = 0;
}
interval:s:30 {
  print(@kafka_runqlat_us);
  exit();
}' 2>/dev/null
# Healthy: P99 < 5ms
# Flag: P99 > 20ms → CPU pressure delaying network/log threads → consumer lag grows
```

### 5.3 Elasticsearch

```bash
# Cluster health
curl -s -u elastic:${ELASTIC_PASSWORD:-changeme} \
  http://localhost:9200/_cluster/health | python3 -m json.tool

# JVM heap + GC across all nodes
curl -s -u elastic:${ELASTIC_PASSWORD:-changeme} \
  'http://localhost:9200/_nodes/stats/jvm?pretty' | python3 -c "
import json, sys
d = json.load(sys.stdin)
for nid, n in d['nodes'].items():
    jvm = n['jvm']['mem']
    gc  = n['jvm']['gc']['collectors']
    print(f\"{n['name']}: heap={jvm['heap_used_percent']}%  \
old_gc={gc['old']['collection_count']}  young_gc={gc['young']['collection_count']}\")
"

# Indexing rate + search latency
curl -s -u elastic:${ELASTIC_PASSWORD:-changeme} \
  'http://localhost:9200/_stats/indexing,search?pretty' | python3 -c "
import json, sys
t = json.load(sys.stdin)['_all']['total']
print(f\"index_total={t['indexing']['index_total']}  \
index_time_ms={t['indexing']['index_time_in_millis']}\")
print(f\"query_total={t['search']['query_total']}  \
query_time_ms={t['search']['query_time_in_millis']}\")
"
```

```bash
# ES page fault rate (Lucene mmap segment access)
sudo bpftrace -e '
software:page-faults:1
/pid == '$ES_PID'/ {
  @es_faults = count();
}
interval:s:10 {
  printf("ES page faults: %d/10s\n", @es_faults);
  clear(@es_faults);
}' 2>/dev/null
# Expected high at startup / after ingest burst (cold segment loading)
# Flag: sustained > 1000/s after warmup → too many small segments; run forcemerge
```

```bash
# Off-CPU latency for ES threads — what is ES blocked on?
sudo bpftrace -e '
tracepoint:sched:sched_switch /args->prev_pid == '$ES_PID'/ {
  @es_offcpu_start = nsecs;
}
tracepoint:sched:sched_switch
/args->next_pid == '$ES_PID' && @es_offcpu_start > 0/ {
  @es_offcpu_us = hist((nsecs - @es_offcpu_start) / 1000);
  @es_offcpu_start = 0;
}
interval:s:30 {
  print(@es_offcpu_us);
  exit();
}' 2>/dev/null
# < 1ms     → normal scheduling
# 1–100ms   → GC pause or merge lock
# > 100ms   → I/O bound (index flush or segment merge)
```

### 5.4 Redis

```bash
# Ops/sec, memory, and network stats
docker exec redis redis-cli INFO stats  | grep -E 'total_commands|instantaneous_ops|total_net'
docker exec redis redis-cli INFO memory | grep -E 'used_memory_human|mem_fragmentation_ratio'

# Slow query log (> 10ms by default)
docker exec redis redis-cli SLOWLOG GET 10
```

```bash
# Redis command receive rate (proxy for ops/sec at kernel level)
sudo bpftrace -e '
kprobe:tcp_recvmsg
/pid == '$REDIS_PID'/ {
  @cmds = count();
}
interval:s:5 {
  printf("Redis recv calls: %d/5s\n", @cmds);
  clear(@cmds);
}' 2>/dev/null
```

```bash
# Redis TCP throughput (useful under redis-benchmark --profile demo)
sudo bpftrace -e '
kprobe:tcp_sendmsg /pid == '$REDIS_PID'/ { @tx = sum(arg2); }
kprobe:tcp_recvmsg /pid == '$REDIS_PID'/ { @rx = sum(arg2); }
interval:s:5 {
  printf("Redis TX: %d bytes/5s  RX: %d bytes/5s\n", @tx, @rx);
  clear(@tx); clear(@rx);
}' 2>/dev/null
```

---

## 6. Continuous Monitoring

### 6.1 Pipeline health dashboard — run for extended sessions
```bash
sudo bpftrace -e '
kprobe:tcp_sendmsg { @tx[comm] = sum(arg2); }
kprobe:tcp_recvmsg { @rx[comm] = sum(arg2); }

software:page-faults:1 { @faults[comm] = count(); }

tracepoint:block:block_rq_issue {
  @blk_start[args->dev, args->sector] = nsecs;
}
tracepoint:block:block_rq_complete
/@blk_start[args->dev, args->sector]/ {
  @blk_lat_ms = hist((nsecs - @blk_start[args->dev, args->sector]) / 1000000);
  @_d = delete(@blk_start[args->dev, args->sector]);
}

interval:s:30 {
  printf("\n=== %s ===\n", strftime("%H:%M:%S", nsecs));
  printf("-- TX bytes/30s --\n");           print(@tx);
  printf("-- RX bytes/30s --\n");           print(@rx);
  printf("-- Page faults/30s --\n");        print(@faults);
  printf("-- Block I/O latency (ms) --\n"); print(@blk_lat_ms);
  clear(@tx); clear(@rx); clear(@faults); clear(@blk_lat_ms); clear(@_d);
}' 2>/dev/null
```

### 6.2 Anomaly alert script — fires on critical conditions
```bash
sudo bpftrace -e '
tracepoint:oom:mark_victim {
  printf("[ALERT] OOM KILL: pid=%d comm=%s  %s\n",
    args->pid, args->comm, strftime("%H:%M:%S", nsecs));
}

tracepoint:block:block_rq_issue {
  @blk_start[args->dev, args->sector] = nsecs;
}
tracepoint:block:block_rq_complete
/@blk_start[args->dev, args->sector]/ {
  $lat_ms = (nsecs - @blk_start[args->dev, args->sector]) / 1000000;
  if ($lat_ms > 200) {
    printf("[ALERT] SLOW I/O: %dms  dev=%d  %s\n",
      $lat_ms, args->dev, strftime("%H:%M:%S", nsecs));
  }
  @_d = delete(@blk_start[args->dev, args->sector]);
}

kprobe:tcp_v4_connect {
  printf("[INFO]  TCP CONNECT: pid=%-6d comm=%s\n", pid, comm);
}' 2>/dev/null
# Leave running during attack profile or load tests
```

---

## 7. Reference

### bpftrace tracepoints replacing BCC tools on kernel 7.x

| BCC tool (broken ≥ 7.x) | bpftrace equivalent | Section |
|---|---|---|
| `runqlat-bpfcc` | `tracepoint:sched:sched_wakeup` + `sched_switch` | 1.1 |
| `cpudist-bpfcc` | `tracepoint:sched:sched_switch` on/off timing | 1.2 |
| `offcputime-bpfcc` | `tracepoint:sched:sched_switch` off-CPU timing | 1.3 |
| `cachestat-bpfcc` | `tracepoint:filemap:mm_filemap_get_pages` + `mm_filemap_add_to_page_cache` | 2.4 |
| `oomkill-bpfcc` | `tracepoint:oom:mark_victim` | 2.5 |
| `tcptop-bpfcc` | `kprobe:tcp_sendmsg` + `tcp_recvmsg` | 3.1, 3.5 |
| `tcplife-bpfcc` | `tracepoint:sock:inet_sock_set_state` | 3.2 |
| `tcpconnect-bpfcc` | `kprobe:tcp_v4_connect` | 3.3 |
| `tcpaccept-bpfcc` | `tracepoint:sock:inet_sock_set_state /newstate==4/` | 3.4 |
| `biolatency-bpfcc` | `tracepoint:block:block_rq_issue` + `block_rq_complete` | 4.1, 4.4 |
| `biotop-bpfcc` | `tracepoint:block:block_rq_issue` with `comm` key | 4.2 |
| `opensnoop-bpfcc` | `tracepoint:syscalls:sys_enter_openat` | 4.3 |

### bpftrace struct paths — kernel 7.x

| Intent | Correct (kernel 7.x) | Wrong (older examples) |
|--------|----------------------|------------------------|
| RSS file pages | `$mm->rss_stat[0].count` | `$mm->rss_stat.count[0].counter` |
| RSS anon pages | `$mm->rss_stat[1].count` | `$mm->rss_stat.count[1].counter` |
| RSS shared mem | `$mm->rss_stat[2].count` | `$mm->rss_stat.count[2].counter` |
| Block I/O start | `tracepoint:block:block_rq_issue` | `kprobe:blk_account_io_start` |
| Block I/O end | `tracepoint:block:block_rq_complete` | `kprobe:blk_account_io_done` |
| delete() return | `@_del = delete(...)` | `delete(...)` (triggers warning) |

### Health thresholds (idle stack)

| Metric | Healthy | Warning | Critical |
|--------|---------|---------|----------|
| Run queue P99 latency | < 1ms | 1–10ms | > 10ms |
| Block I/O P99 latency | < 5ms | 5–50ms | > 100ms |
| Page cache hit rate | > 95% | 85–95% | < 85% |
| ES heap used | < 70% | 70–85% | > 85% |
| ES old GC count (delta) | 0 | 1–3/min | > 5/min |
| Kafka consumer lag | 0 | 1–1000 | > 1000 (growing) |
| OOM kills | 0 | — | any |
| RSS swap | 0 | any non-zero | — |
| FB proc_records delta | positive | stuck at 0 | 0 with no errors |

### Kali-specific notes

```bash
# BCC tools have the -bpfcc suffix on Kali/Debian
ls /usr/sbin/*-bpfcc

# BCC broken on kernel 7.x — fix requires either:
#   a) Downgrade to kernel < 7.x
#   b) Build bpfcc-tools from source against kernel 7.x headers
#   c) Use the bpftrace equivalents in this playbook (recommended)

# If bpftrace can't find BTF:
ls /sys/kernel/btf/vmlinux   # must exist
# If missing: sudo apt-get install linux-image-$(uname -r)-dbg
```
