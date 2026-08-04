# obsdocker Performance Baseline & Attack Comparison Playbook

**Elasticsearch  •  Apache Kafka  •  Fluent Bit**  
**ftrace  •  bpftrace  •  perf  •  System Commands**

---
| Field | Value |
| --- | --- |
| Purpose | Capture performance baseline before attack profile, repeat during attack, and diff for anomaly detection |
| Target Services | Elasticsearch (es01), Apache Kafka (kafka1), Fluent Bit (fluent-bit) |
| Attack Profiles | log-injector, recon (nmap), log-generator (control) |
| Tools Used | ftrace, kprobes, bpftrace, perf stat, perf sched, ss, podman stats, cgroup fs |
| Output Dirs | ~/baseline/  and  ~/attack/ |
| Kernel Requirement | Linux 5.x+, tracefs mounted, bpftrace installed |

# Phase 0 — Environment Setup

Run these steps once before any captures. Verify the stack is healthy and all tools are available.

## 0.1  Verify Stack Health

```bash
# All containers must be healthy before baseline capture
podman ps --format "table {{.Names}}\t{{.Status}}"

# Expected: all services show 'healthy' or 'Up'
# kafka1, es01, fluent-bit, kafka-connect, kibana, grafana, kafka-ui
```

```bash
# Verify no attack profiles are running
podman ps | grep -E 'recon|log-injector|log-generator'
# Should return empty
```

## 0.2  Collect Container PIDs

```bash
# Capture all service PIDs — used throughout this playbook
ES_PID=$(podman inspect --format '{{.State.Pid}}' es01)
KAFKA_PID=$(podman inspect --format '{{.State.Pid}}' kafka1)
FB_PID=$(podman inspect --format '{{.State.Pid}}' fluent-bit)
KC_PID=$(podman inspect --format '{{.State.Pid}}' kafka-connect)

echo "es01=$ES_PID  kafka1=$KAFKA_PID  fluent-bit=$FB_PID  kafka-connect=$KC_PID"

# Capture cgroup paths for each service
ES_CG=$(cat /proc/$ES_PID/cgroup | head -1 | cut -d: -f3)
KAFKA_CG=$(cat /proc/$KAFKA_PID/cgroup | head -1 | cut -d: -f3)
FB_CG=$(cat /proc/$FB_PID/cgroup | head -1 | cut -d: -f3)
```

## 0.3  Verify ftrace and Tools

```bash
# Confirm tracefs is mounted
mount | grep tracefs || sudo mount -t tracefs nodev /sys/kernel/tracing

# Check available tracers
cat /sys/kernel/tracing/available_tracers
# Required: function function_graph nop

# Verify event-fork option exists
ls /sys/kernel/tracing/options/event-fork

# Verify bpftrace
sudo bpftrace --version

# Verify perf
sudo perf stat echo ok
```

## 0.4  Prepare Output Directories

```bash
mkdir -p ~/baseline/ftrace ~/baseline/system ~/baseline/kafka
mkdir -p ~/attack/ftrace ~/attack/system ~/attack/kafka

# Increase ftrace ring buffer to avoid overrun
sudo sh -c 'echo 16384 > /sys/kernel/tracing/buffer_size_kb'
```

> NOTE: Export all PID variables in the same shell session you will run captures from. Variables do not persist across terminals.

# Phase 1 — Baseline Capture (Pre-Attack)

Execute all sections below with the stack idle and zero attack containers running. Each section targets a specific subsystem. Run sequentially.

## 1.1  System Snapshot

Point-in-time system state — captured first as the authoritative idle baseline.
```bash
OUT=~/baseline/system/snapshot.txt
echo '=== DATE ===' > $OUT && date >> $OUT
echo '=== UPTIME ===' >> $OUT && uptime >> $OUT
echo '=== VMSTAT ===' >> $OUT && vmstat 1 10 >> $OUT
echo '=== IOSTAT ===' >> $OUT && iostat -x 1 5 >> $OUT
echo '=== FREE ===' >> $OUT && free -h >> $OUT
echo '=== SS SUMMARY ===' >> $OUT && ss -s >> $OUT
echo '=== SS TCP ===' >> $OUT && ss -tnp >> $OUT
echo '=== PODMAN STATS ===' >> $OUT
podman stats --no-stream \
--format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}" >> $OUT
echo '=== ES CLUSTER HEALTH ===' >> $OUT
curl -s http://localhost:9200/_cluster/health | python3 -m json.tool >> $OUT
echo '=== ES INDICES ===' >> $OUT
curl -s http://localhost:9200/_cat/indices?v >> $OUT
echo '=== KAFKA TOPICS ===' >> $OUT
podman exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list >> $OUT
```

## 1.2  CPU Baseline — perf stat

Split into three groups to avoid PMU counter multiplexing errors. Each group fits within the 4–8 hardware PMU slots available per core.

### Group 1 — Execution Quality (IPC + Cache)

```bash
sudo perf stat -a \
-e '{cycles,instructions}' \
-e '{cache-misses,cache-references}' \
-- sleep 30 2>&1 | tee ~/baseline/system/perf-cpu-group1.txt
```

### Group 2 — Branch Predictor

```bash
sudo perf stat -a \
-e '{branch-instructions,branch-misses}' \
-- sleep 30 2>&1 | tee ~/baseline/system/perf-cpu-group2.txt
```

### Group 3 — Scheduler Pressure

```bash
sudo perf stat -a \
-e '{context-switches,cpu-migrations,page-faults}' \
-- sleep 30 2>&1 | tee ~/baseline/system/perf-cpu-group3.txt
```

> NOTE: Do NOT combine all 9 events in one perf stat call — PMU multiplexing will produce invalid ratios (e.g. 821M branch-misses from 6K instructions).

## 1.3  Scheduler Latency — perf sched

```bash
sudo perf sched record -o ~/baseline/system/perf-sched.data -- sleep 30

# Per-task latency summary
sudo perf sched latency -i ~/baseline/system/perf-sched.data \
2>&1 | tee ~/baseline/system/perf-sched-latency.txt

# Time history — wait/schedule/run columns per task
sudo perf sched timehist -i ~/baseline/system/perf-sched.data \
2>&1 | head -200 | tee ~/baseline/system/perf-sched-timehist.txt
```

## 1.4  Memory Baseline

### RSS + PSS per Container

```bash
OUT=~/baseline/system/mem-per-container.txt
for name in kafka1 es01 fluent-bit kafka-connect kibana grafana; do
PID=$(podman inspect --format '{{.State.Pid}}' $name 2>/dev/null)
[ -z "$PID" ] && continue
RSS=$(awk '/^VmRSS/{print $2}' /proc/$PID/status 2>/dev/null)
PSS=$(awk '/^Pss:/{sum+=$2} END{print sum}' /proc/$PID/smaps 2>/dev/null)
SWAP=$(awk '/^VmSwap/{print $2}' /proc/$PID/status 2>/dev/null)
echo "$name (pid $PID): RSS=${RSS}kB PSS=${PSS}kB SWAP=${SWAP}kB"
done | tee $OUT
```

### cgroup Memory Accounting (Most Accurate)

```bash
OUT=~/baseline/system/mem-cgroup.txt
for name in kafka1 es01 fluent-bit kafka-connect kibana grafana; do
PID=$(podman inspect --format '{{.State.Pid}}' $name 2>/dev/null)
[ -z "$PID" ] && continue
CG=$(cat /proc/$PID/cgroup | head -1 | cut -d: -f3)
BASE="/sys/fs/cgroup${CG}"
MEM=$(cat $BASE/memory.current 2>/dev/null)
OOM=$(awk '/oom_kill/{print $2}' $BASE/memory.events 2>/dev/null)
echo "$name: MEM=$((MEM/1024/1024))MB OOM_kills=${OOM:-0}"
done | tee $OUT
```

### bpftrace Page Fault Rate

```bash
sudo bpftrace -e '
software:page-faults:1 {
@faults[comm] = count();
}
interval:s:30 {
print(@faults);
exit();
}' 2>&1 | tee ~/baseline/system/bpf-page-faults.txt
```

## 1.5  Network Baseline

### TCP Connection Rate

```bash
sudo bpftrace -e '
kprobe:tcp_v4_connect {
@connects[comm] = count();
}
interval:s:30 {
print(@connects);
clear(@connects);
exit();
}' 2>&1 | tee ~/baseline/system/bpf-tcp-connects.txt
```

### TCP Throughput per Process

```bash
sudo bpftrace -e '
kprobe:tcp_sendmsg { @send_bytes[comm] = sum(arg2); }
kprobe:tcp_recvmsg { @recv_bytes[comm] = sum(arg2); }
interval:s:30 {
print(@send_bytes);
print(@recv_bytes);
exit();
}' 2>&1 | tee ~/baseline/system/bpf-tcp-throughput.txt
```

### Socket State Snapshot

```bash
ss -tnp > ~/baseline/system/ss-tcp-full.txt
ss -s    > ~/baseline/system/ss-summary.txt
cat ~/baseline/system/ss-summary.txt
```

## 1.6  Disk I/O Baseline

### Block I/O Latency Histogram

```bash
sudo bpftrace -e '
kprobe:blk_account_io_start { @start[arg0] = nsecs; }
kprobe:blk_account_io_done /@start[arg0]/ {
@io_lat_us = hist((nsecs - @start[arg0]) / 1000);
delete(@start[arg0]);
}
interval:s:30 {
print(@io_lat_us);
exit();
}' 2>&1 | tee ~/baseline/system/bpf-blk-latency.txt
```

### File Open Patterns

```bash
sudo bpftrace -e '
tracepoint:syscalls:sys_enter_openat {
@opens[comm] = count();
}
interval:s:30 {
print(@opens);
exit();
}' 2>&1 | tee ~/baseline/system/bpf-file-opens.txt
```

## 1.7  Elasticsearch Baseline (es01)

### CPU Profiling — perf record

```bash
# JVM: add -XX:+PreserveFramePointer to ES_JAVA_OPTS for readable stacks
sudo perf record -F 99 -p $ES_PID -g --call-graph dwarf \
-o ~/baseline/system/perf-es01.data -- sleep 30
sudo perf report -i ~/baseline/system/perf-es01.data \
--stdio > ~/baseline/system/perf-es01-report.txt
```

### ftrace — Page Faults During Lucene mmap Access

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace && echo > kprobe_events

# Clear previous tracing configuration
echo 0 > tracing_on
echo > trace

# Enable user-space page fault events
echo 1 > events/exceptions/page_fault_user/enable

# Enable tracing
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/baseline/system/page_faults.txt
"
```

### ftrace — VFS Write Latency (Index Flushing)

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace && echo > kprobe_events
echo 'p:es_write vfs_write count=%dx'     >> kprobe_events
echo 'r:es_write_ret vfs_write ret=$retval' >> kprobe_events
echo 1 > events/kprobes/es_write/enable
echo 1 > events/kprobes/es_write_ret/enable
echo "common_pid == $ES_PID" > events/kprobes/es_write/filter
echo "common_pid == $ES_PID" > events/kprobes/es_write_ret/filter
echo 1 > options/event-fork
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/baseline/ftrace/es-vfs-write.txt
echo 0 > events/kprobes/enable
echo > kprobe_events
echo 0 > options/event-fork
"
```

### ES Segment Merge Stats

```bash
curl -s 'http://localhost:9200/_stats/merge?pretty' \
| python3 -c "
import json,sys
s=json.load(sys.stdin)
m=s['_all']['total']['merges']
print(f\"total={m['total']} docs={m['total_docs']} \\
size={m['total_size']} time={m['total_time']}\")" \
| tee ~/baseline/system/es-merge-stats.txt

# Segment count per index
curl -s 'http://localhost:9200/_cat/segments?v' \
| tee ~/baseline/system/es-segments.txt
```

### ES JVM Heap Stats

```bash
curl -s 'http://localhost:9200/_nodes/stats/jvm?pretty' \
| python3 -c "
import json,sys
d=json.load(sys.stdin)
for nid,n in d['nodes'].items():
jvm=n['jvm']['mem']
gc=n['jvm']['gc']['collectors']
print(f\"heap_used={jvm['heap_used_percent']}% \\
old_gc={gc['old']['collection_count']} \\
young_gc={gc['young']['collection_count']}\")" \
| tee ~/baseline/system/es-jvm-stats.txt
```

## 1.8  Kafka Baseline (kafka1)

### CPU Profiling — perf record

```bash
sudo perf record -F 99 -p $KAFKA_PID -g --call-graph dwarf \
-o ~/baseline/system/perf-kafka1.data -- sleep 30
sudo perf report -i ~/baseline/system/perf-kafka1.data \
--stdio > ~/baseline/system/perf-kafka1-report.txt
```

### ftrace — Disk Write Latency (Log Segment Writes)

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace && echo > kprobe_events
echo 'p:kafka_write vfs_write count=%dx'      >> kprobe_events
echo 'r:kafka_write_ret vfs_write ret=$retval' >> kprobe_events
echo 1 > events/kprobes/kafka_write/enable
echo 1 > events/kprobes/kafka_write_ret/enable
echo "common_pid == $KAFKA_PID" > events/kprobes/kafka_write/filter
echo "common_pid == $KAFKA_PID" > events/kprobes/kafka_write_ret/filter
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/baseline/ftrace/kafka-vfs-write.txt
echo 0 > events/kprobes/enable
echo > kprobe_events
"
```

### ftrace — TCP Send Latency (Producer Path)

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace && echo > kprobe_events
echo 'p:kafka_tcp_send tcp_sendmsg'          >> kprobe_events
echo 'r:kafka_tcp_send_ret tcp_sendmsg ret=\$retval' >> kprobe_events
echo 1 > events/kprobes/kafka_tcp_send/enable
echo 1 > events/kprobes/kafka_tcp_send_ret/enable
echo 'common_pid == $KAFKA_PID' > events/kprobes/kafka_tcp_send/filter
echo 'common_pid == $KAFKA_PID' > events/kprobes/kafka_tcp_send_ret/filter
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/baseline/ftrace/kafka-tcp-send.txt
echo 0 > events/kprobes/enable
echo > kprobe_events
"
```

### Kafka Pipeline Stats

```bash
# Consumer group lag
podman exec kafka1 kafka-consumer-groups \
--bootstrap-server localhost:9092 \
--describe --all-groups \
2>&1 | tee ~/baseline/kafka/consumer-lag.txt

# Topic offsets
podman exec kafka1 kafka-run-class kafka.tools.GetOffsetShell \
--bootstrap-server localhost:9092 --time -1 \
2>&1 | tee ~/baseline/kafka/topic-offsets.txt

# Topic list and partition details
podman exec kafka1 kafka-topics \
--bootstrap-server localhost:9092 --describe \
2>&1 | tee ~/baseline/kafka/topic-details.txt
```

## 1.9  Fluent Bit Baseline (fluent-bit)

### CPU Profiling — perf record

```bash
sudo perf record -F 99 -p $FB_PID -g --call-graph dwarf \
-o ~/baseline/system/perf-fluent-bit.data -- sleep 30
sudo perf report -i ~/baseline/system/perf-fluent-bit.data \
--stdio > ~/baseline/system/perf-fluent-bit-report.txt
```

### ftrace — TCP Send Latency (Kafka Producer from FB)

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace && echo > kprobe_events
echo 'p:fb_tcp_send tcp_sendmsg size=%dx'         >> kprobe_events
echo 'r:fb_tcp_send_ret tcp_sendmsg ret=\$retval' >> kprobe_events
echo 1 > events/kprobes/fb_tcp_send/enable
echo 1 > events/kprobes/fb_tcp_send_ret/enable
echo 'common_pid == $FB_PID' > events/kprobes/fb_tcp_send/filter
echo 'common_pid == $FB_PID' > events/kprobes/fb_tcp_send_ret/filter
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/baseline/ftrace/fb-tcp-send.txt
echo 0 > events/kprobes/enable
echo > kprobe_events
"
```

### ftrace — File Opens (Log Tail Activity)

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace
echo 1 > events/syscalls/sys_enter_openat/enable
echo 'common_pid == $FB_PID' > events/syscalls/sys_enter_openat/filter
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/baseline/ftrace/fb-file-opens.txt
echo 0 > events/syscalls/sys_enter_openat/enable
"
```

### Fluent Bit Internal Metrics

```bash
# Plugin metrics — records in/out per plugin
curl -s http://localhost:2020/api/v1/metrics | python3 -m json.tool \
| tee ~/baseline/system/fb-metrics.txt

# Health check
curl -s http://localhost:2020/api/v1/health | tee ~/baseline/system/fb-health.txt
```

## 1.10  Scheduler Tracing — Pipeline PIDs

Trace scheduler events filtered to all pipeline service PIDs together.
```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace
echo 1 > events/sched/sched_wakeup/enable
echo 1 > events/sched/sched_switch/enable
echo 1 > events/sched/sched_migrate_task/enable
echo '$ES_PID $KAFKA_PID $FB_PID $KC_PID' > set_event_pid
echo 1 > options/event-fork
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/baseline/ftrace/sched-pipeline.txt
echo 0 > events/sched/enable
echo > set_event_pid
echo 0 > options/event-fork
"
```

## 1.11  Archive Baseline

```bash
STAMP=$(date +%Y%m%d_%H%M%S)
tar -czf ~/baseline-${STAMP}.tar.gz ~/baseline/
echo "Baseline archived: ~/baseline-${STAMP}.tar.gz"
ls -lh ~/baseline-${STAMP}.tar.gz
```

> BASELINE COMPLETE — Verify all output files exist before proceeding to Phase 2.

## Baseline Capture Checklist

| # | Artifact | Path | Done |
| --- | --- | --- | --- |
| 1 | System snapshot | ~/baseline/system/snapshot.txt | ☐ |
| 2 | perf stat group 1 (IPC+cache) | ~/baseline/system/perf-cpu-group1.txt | ☐ |
| 3 | perf stat group 2 (branch) | ~/baseline/system/perf-cpu-group2.txt | ☐ |
| 4 | perf stat group 3 (sched) | ~/baseline/system/perf-cpu-group3.txt | ☐ |
| 5 | perf sched latency | ~/baseline/system/perf-sched-latency.txt | ☐ |
| 6 | perf sched timehist | ~/baseline/system/perf-sched-timehist.txt | ☐ |
| 7 | Memory RSS/PSS | ~/baseline/system/mem-per-container.txt | ☐ |
| 8 | cgroup memory | ~/baseline/system/mem-cgroup.txt | ☐ |
| 9 | Page fault rate | ~/baseline/system/bpf-page-faults.txt | ☐ |
| 10 | TCP connect rate | ~/baseline/system/bpf-tcp-connects.txt | ☐ |
| 11 | TCP throughput | ~/baseline/system/bpf-tcp-throughput.txt | ☐ |
| 12 | SS TCP snapshot | ~/baseline/system/ss-tcp-full.txt | ☐ |
| 13 | Block I/O latency | ~/baseline/system/bpf-blk-latency.txt | ☐ |
| 14 | File open patterns | ~/baseline/system/bpf-file-opens.txt | ☐ |
| 15 | ES page faults (ftrace) | ~/baseline/ftrace/es-page-fault.txt | ☐ |
| 16 | ES VFS write (ftrace) | ~/baseline/ftrace/es-vfs-write.txt | ☐ |
| 17 | ES segment stats | ~/baseline/system/es-segments.txt | ☐ |
| 18 | ES merge stats | ~/baseline/system/es-merge-stats.txt | ☐ |
| 19 | ES JVM heap stats | ~/baseline/system/es-jvm-stats.txt | ☐ |
| 20 | Kafka VFS write (ftrace) | ~/baseline/ftrace/kafka-vfs-write.txt | ☐ |
| 21 | Kafka TCP send (ftrace) | ~/baseline/ftrace/kafka-tcp-send.txt | ☐ |
| 22 | Kafka consumer lag | ~/baseline/kafka/consumer-lag.txt | ☐ |
| 23 | Kafka topic offsets | ~/baseline/kafka/topic-offsets.txt | ☐ |
| 24 | Fluent Bit TCP send (ftrace) | ~/baseline/ftrace/fb-tcp-send.txt | ☐ |
| 25 | Fluent Bit file opens (ftrace) | ~/baseline/ftrace/fb-file-opens.txt | ☐ |
| 26 | Fluent Bit metrics | ~/baseline/system/fb-metrics.txt | ☐ |
| 27 | Pipeline scheduler trace | ~/baseline/ftrace/sched-pipeline.txt | ☐ |

# Phase 2 — Attack Capture

Start attack profiles then immediately re-run the same captures from Phase 1 targeting ~/attack/ directories. Run all three attack profiles simultaneously for maximum signal.

## 2.1  Start Attack Profiles

```bash
# Start all attack containers
podman-compose --profile attack up -d

# Optionally start log-generator as control traffic
podman-compose --profile demo up -d

# Verify attack containers are running
podman ps | grep -E 'recon|log-injector|log-generator'

# Wait for recon to start scanning
sleep 15

# Capture recon PID for fork-aware tracing
RECON_PID=$(podman inspect --format '{{.State.Pid}}' recon)
INJECTOR_PID=$(podman inspect --format '{{.State.Pid}}' log-injector)
echo "recon=$RECON_PID  log-injector=$INJECTOR_PID"
```

## 2.2  Repeat All Phase 1 Captures

> Run every command from Phase 1 sections 1.1 through 1.10, substituting ~/baseline/ with ~/attack/ in all output paths. The commands are identical — only the destination directory changes.

Quick substitution — run this to create attack capture scripts from baseline scripts:
```bash
# Auto-generate attack capture scripts from baseline scripts
# (if you saved them as .sh files)
sed 's|~/baseline/|~/attack/|g' ~/baseline-capture.sh > ~/attack-capture.sh
chmod +x ~/attack-capture.sh
```

## 2.3  Recon-Specific Captures (nmap)

These captures are attack-phase only — no baseline equivalent needed since recon container doesn't exist at baseline.

### Inbound SYN Rate — Services Being Scanned

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace && echo > kprobe_events
echo 'p:syn_recv tcp_v4_syn_recv_sock'  >> kprobe_events
echo 'p:tcp_drop_probe tcp_drop'        >> kprobe_events
echo 1 > events/kprobes/syn_recv/enable
echo 1 > events/kprobes/tcp_drop_probe/enable
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/attack/ftrace/recon-syn-rate.txt
echo 0 > events/kprobes/enable
echo > kprobe_events
"
```

### Full Recon Process Tree — Fork-Aware Tracing

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace
echo nop > current_tracer

# Fork and exec events — follow entire nmap process tree
echo 1 > events/sched/sched_process_fork/enable
echo 1 > events/sched/sched_process_exec/enable
echo 1 > events/sched/sched_process_exit/enable
echo 1 > events/syscalls/sys_enter_connect/enable
echo 1 > events/syscalls/sys_enter_execve/enable
echo 1 > events/syscalls/sys_enter_sendto/enable

# Seed with recon PID and follow all children
echo $RECON_PID > set_event_pid
echo 1 > options/event-fork

echo 1 > tracing_on
sleep 60
echo 0 > tracing_on
cp trace ~/attack/ftrace/recon-process-tree.txt

echo 0 > events/sched/enable
echo 0 > events/syscalls/enable
echo 0 > options/event-fork
echo > set_event_pid
"
```

### TCP State Machine Under Scan

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace
echo 1 > events/sock/inet_sock_set_state/enable
echo 1 > events/tcp/tcp_retransmit_skb/enable
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/attack/ftrace/recon-tcp-state.txt
echo 0 > events/sock/inet_sock_set_state/enable
echo 0 > events/tcp/tcp_retransmit_skb/enable
"
```

### nsenter — Trace Inside Recon Namespace

```bash
# Trace from inside recon's network + PID namespace
# Catches all nmap child processes automatically
sudo nsenter -t $RECON_PID --pid --net --mount \
bpftrace -e '
tracepoint:sched:sched_process_fork {
printf("FORK  parent=%-6d child=%d\n",
args->parent_pid, args->child_pid);
}
tracepoint:sched:sched_process_exec {
printf("EXEC  pid=%-6d %s\n", pid, str(args->filename));
}
tracepoint:syscalls:sys_enter_connect {
printf("CONN  pid=%-6d comm=%s\n", pid, comm);
}
tracepoint:syscalls:sys_enter_sendto {
printf("SEND  pid=%-6d len=%d\n", pid, args->len);
}
tracepoint:sched:sched_process_exit {
printf("EXIT  pid=%-6d comm=%s\n", pid, comm);
}' | tee ~/attack/ftrace/recon-nsenter.txt &
# runs in background — kill after 60s
sleep 60 && sudo kill %1
```

## 2.4  Log Injector Specific Captures

### Socket Buffer Pressure — 200KB Blob Detection

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace && echo > kprobe_events
# Fires when a socket exceeds its memory budget
echo 'p:sk_mem_pressure __sk_mem_raise_allocated size=%dx' >> kprobe_events
echo 1 > events/kprobes/sk_mem_pressure/enable
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/attack/ftrace/log-injector-sk-mem.txt
echo 0 > events/kprobes/sk_mem_pressure/enable
echo > kprobe_events
"
```

### Fluent Bit HTTP Ingest Latency

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on && echo > trace && echo > kprobe_events
echo 'p:fb_send tcp_sendmsg size=%dx'         >> kprobe_events
echo 'r:fb_send_ret tcp_sendmsg ret=\$retval' >> kprobe_events
echo 1 > events/kprobes/fb_send/enable
echo 1 > events/kprobes/fb_send_ret/enable
echo 'common_pid == $FB_PID' > events/kprobes/fb_send/filter
echo 'common_pid == $FB_PID' > events/kprobes/fb_send_ret/filter
echo 1 > tracing_on
sleep 30
echo 0 > tracing_on
cp trace ~/attack/ftrace/log-injector-fb-send.txt
echo 0 > events/kprobes/enable
echo > kprobe_events
"
```

# Phase 3 — Diff and Analysis

Compare baseline and attack captures to identify anomalies. Run after both phases are complete.

## 3.1  Quick Diff Script

```bash
#!/bin/bash
# Run from home directory after both phases complete

echo '========================================'
echo '  obsdocker Baseline vs Attack Summary'
echo '========================================'

echo ''
echo '--- TCP Connect Rate ---'
echo -n 'BASELINE: ' && grep -c 'connects' ~/baseline/system/bpf-tcp-connects.txt 2>/dev/null
echo -n 'ATTACK:   ' && grep -c 'connects' ~/attack/system/bpf-tcp-connects.txt 2>/dev/null

echo ''
echo '--- SYN Rate (recon) ---'
echo -n 'ATTACK SYN count: ' && grep -c 'syn_recv' ~/attack/ftrace/recon-syn-rate.txt 2>/dev/null
echo -n 'ATTACK DROP count: ' && grep -c 'tcp_drop' ~/attack/ftrace/recon-syn-rate.txt 2>/dev/null

echo ''
echo '--- Socket Memory Pressure ---'
echo -n 'BASELINE: ' && grep -c 'sk_mem_pressure' ~/baseline/ftrace/fb-tcp-send.txt 2>/dev/null || echo 0
echo -n 'ATTACK:   ' && grep -c 'sk_mem_pressure' ~/attack/ftrace/log-injector-sk-mem.txt 2>/dev/null

echo ''
echo '--- ES Page Faults ---'
echo -n 'BASELINE: ' && grep -c 'es_fault' ~/baseline/ftrace/es-page-fault.txt 2>/dev/null
echo -n 'ATTACK:   ' && grep -c 'es_fault' ~/attack/ftrace/es-page-fault.txt 2>/dev/null

echo ''
echo '--- Kafka VFS Writes ---'
echo -n 'BASELINE: ' && grep -c 'kafka_write:' ~/baseline/ftrace/kafka-vfs-write.txt 2>/dev/null
echo -n 'ATTACK:   ' && grep -c 'kafka_write:' ~/attack/ftrace/kafka-vfs-write.txt 2>/dev/null

echo ''
echo '--- CPU Migrations (pipeline) ---'
echo -n 'BASELINE: ' && grep -c 'sched_migrate' ~/baseline/ftrace/sched-pipeline.txt 2>/dev/null
echo -n 'ATTACK:   ' && grep -c 'sched_migrate' ~/attack/ftrace/sched-pipeline.txt 2>/dev/null

echo ''
echo '--- Kafka Consumer Lag ---'
echo '= BASELINE =' && cat ~/baseline/kafka/consumer-lag.txt
echo '= ATTACK ='   && cat ~/attack/kafka/consumer-lag.txt

echo ''
echo '--- OOM Kill Events ---'
for name in kafka1 es01 fluent-bit; do
PID=$(podman inspect --format '{{.State.Pid}}' $name 2>/dev/null)
CG=$(cat /proc/$PID/cgroup 2>/dev/null | head -1 | cut -d: -f3)
OOM=$(awk '/oom_kill/{print $2}' /sys/fs/cgroup${CG}/memory.events 2>/dev/null)
echo "$name OOM kills: ${OOM:-0}"
done
```

## 3.2  Key Metrics Correlation Table

| Metric | Baseline Signal | Attack Delta Meaning | Source File |
| --- | --- | --- | --- |
| IPC (instr/cycle) | 1.5–2.5 at idle | Drop → CPU stalling on memory/locks | perf-cpu-group1.txt |
| Cache miss rate | < 5% LLC miss | Spike → ES merge pressure or heap churn | perf-cpu-group1.txt |
| Branch miss rate | < 1% | Spike → malformed JSON hitting parser | perf-cpu-group2.txt |
| Context switches | Low, steady | Spike → I/O blocking under attack load | perf-cpu-group3.txt |
| CPU migrations | Occasional | Frequent → cache locality destroyed | perf-cpu-group3.txt |
| tcp_v4_connect rate | < 5/s inter-container | Spike → nmap SYN flood | bpf-tcp-connects.txt |
| tcp_retransmit_skb | ~0 | Non-zero → port scan induced retransmits | recon-tcp-state.txt |
| inet_sock_set_state | Low (full handshakes) | Drops → SYN scan (no full handshake) | recon-tcp-state.txt |
| sk_mem_raise | ~0 | Spikes → 200KB blob exhausting socket buffer | log-injector-sk-mem.txt |
| ES page faults | Low — warm mmap | Spike → new segments from flood ingest | es-page-fault.txt |
| ES vfs_write rate | Periodic flushes | Continuous → high ingest rate | es-vfs-write.txt |
| Kafka vfs_write rate | Steady log appends | Increase → higher produce throughput | kafka-vfs-write.txt |
| Consumer lag | ~0 | Growing → pipeline falling behind | consumer-lag.txt |
| FB tcp_sendmsg lat | Low, consistent | Increases → Kafka backpressure | fb-tcp-send.txt |
| do_nanosleep count | High — processes idle | Drops → processes too busy to sleep | sched-pipeline.txt |
| sched_migrate_task | Low | High → scheduler thrashing under load | sched-pipeline.txt |
| OOM kills | 0 | Any non-zero → memory exhaustion | cgroup memory.events |

## 3.3  nmap Phase Boundary Analysis

Extract exact phase transition timestamps from the recon process tree trace:
```bash
# Extract nmap execve timestamps — each line = one nmap phase start
grep 'sched_process_exec' ~/attack/ftrace/recon-process-tree.txt \
| grep 'nmap' \
| awk '{print $3, $NF}'

# Calculate phase durations
grep 'sched_process_exec' ~/attack/ftrace/recon-process-tree.txt \
| grep 'nmap' \
| awk '{print $3}' | sed 's/://' \
| awk 'NR>1{printf "phase duration: %.3fs\n", $1-prev} {prev=$1}'
```

## 3.4  RST to Connect Ratio — SYN Scan Confirmation

```bash
CONNECTS=$(grep -c 'recon_connect:'  ~/attack/ftrace/recon-syn-rate.txt 2>/dev/null || echo 0)
RSTS=$(grep -c 'tcp_send_active_reset' ~/attack/ftrace/recon-syn-rate.txt 2>/dev/null || echo 0)
echo "connects=$CONNECTS  RSTs=$RSTS"
echo "RST/connect ratio: $(echo "scale=2; $RSTS / ($CONNECTS + 1)" | bc)"
# Ratio ~1.0 confirms pure SYN scan — RST after every SYN-ACK
```

# Reference

## ftrace Cleanup — Run After Every Capture

```bash
sudo sh -c "
cd /sys/kernel/tracing
echo 0 > tracing_on
echo nop > current_tracer
echo 0 > events/enable
echo > kprobe_events
echo > set_event_pid
echo 0 > options/event-fork
echo 0 > options/function-fork
echo 1408 > buffer_size_kb
echo > trace
"
```

## Common Rocky Linux Issues

| Symptom | Cause | Fix |
| --- | --- | --- |
| trace file empty despite events enabled | PID filter missing child processes | Add 'echo 1 > options/event-fork' before tracing |
| tcp_connect tracepoint empty | Container in separate network namespace | Use nsenter -t $PID --net bpftrace ... |
| sys_enter_execve empty for nmap | nmap runs as child PID not in filter | Use event-fork option or nsenter |
| bpftrace: permission denied | Not running as root | sudo bpftrace or add user to tracing group |
| perf: paranoid level too high | /proc/sys/kernel/perf_event_paranoid = 3 | sudo sysctl kernel.perf_event_paranoid=1 |
| Branch miss ratio > 100% | PMU multiplexing with too many events | Split into groups with {} syntax |
| blk_account_io_start not found | Kernel >= 5.15 renamed the function | Use tracepoint:block:block_rq_issue instead |
| do_user_addr_fault not found | Kernel version dependent name | Check: sudo bpftrace -l 'kprobe:*fault*' |

## Key Kernel Functions Reference

| Function | Subsystem | What it probes |
| --- | --- | --- |
| tcp_sendmsg | Network | TCP send — fluent-bit → kafka, kafka → connect |
| tcp_v4_connect | Network | Outbound TCP connection initiation |
| tcp_v4_syn_recv_sock | Network | Inbound SYN accepted — nmap scan rate |
| tcp_drop | Network | Packet dropped — SYN backlog overflow |
| tcp_send_active_reset | Network | RST sent — nmap SYN scan signature |
| __sk_mem_raise_allocated | Network | Socket buffer over budget — blob payload pressure |
| vfs_write | VFS | All file writes — Kafka log segments, ES index flush |
| do_user_addr_fault | Memory | Page fault — ES Lucene mmap segment access |
| __sk_mem_raise_allocated | Memory | Socket memory pressure — oversized payload |
| blk_account_io_start/done | Block I/O | Block I/O latency histogram |
| tcp_retransmit_skb | Network | TCP retransmit — port scan induced |

## MITRE ATT&CK Mapping

| Attack Container | Technique | ATT&CK ID | Observable Signal |
| --- | --- | --- | --- |
| recon | Network Service Discovery | T1046 | tcp_v4_syn_recv_sock spike, TIME_WAIT surge |
| recon | OS Fingerprinting | T1082 | NSE script execve events, http-enum probes |
| log-injector | Log Tampering | T1565.001 | Spoofed host fields in ES — search for host:kibana or host:es01 |
| log-injector | Stored XSS | T1059.007 | <script> tags in msg field → Kibana dashboard injection |
| log-injector | SQL Injection | T1190 | OR 1=1 pattern in query field |
| log-injector | Resource Exhaustion | T1499 | sk_mem_raise spike, ES segment merge pressure |
