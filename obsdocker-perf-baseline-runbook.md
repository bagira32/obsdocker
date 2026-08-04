# obsdocker Stack — Performance Baseline Runbook

**Purpose:** Capture a pre-attack performance baseline using `perf`, `ftrace`, and `bpftrace` so post-attack comparisons are meaningful.  
**When to run:** Stack fully up, all containers healthy, zero attack profiles active.  
**Estimated time:** ~30 minutes for full capture.

---

## Prerequisites

```bash
# Verify stack is healthy, no attack profiles running
podman ps --format "table {{.Names}}\t{{.Status}}"

# Install tools if missing (Fedora/RHEL)
sudo dnf install -y perf bpftrace kernel-devel

# Verify bpftrace
sudo bpftrace --version

# Confirm perf works
sudo perf stat echo ok

# Create output directory
mkdir -p ~/baseline && cd ~/baseline
```

---

## 1. System-Wide CPU Baseline (`perf stat`)

Capture aggregate CPU counters for the whole stack at idle.

```bash
# 60-second system-wide stat snapshot
# Group 1 — execution quality
sudo perf stat -a \
  -e cycles,instructions,cache-misses,cache-references \
  -- sleep 30 2>&1 | tee -a ~/baseline/perf-stat-baseline.txt

# Group 2 — branch predictor only
sudo perf stat -a \
  -e branch-instructions,branch-misses \
  -- sleep 30 2>&1 | tee -a ~/baseline/perf-stat-baseline.txt

# Group 3 — scheduler
sudo perf stat -a \
  -e context-switches,cpu-migrations,page-faults \
  -- sleep 30 2>&1 | tee -a ~/baseline/perf-stat-baseline.txt
```

**What to record:**
- IPC (instructions per cycle) — expect > 1.0 at idle
- Cache miss rate — flag if > 5% LLC miss rate
- Context switch rate — baseline for scheduler pressure

---

## 2. Per-Container CPU Profiling (`perf top` / `perf record`)

Profile each critical container individually.

```bash
# Get container PIDs
for name in kafka1 es01 fluent-bit kafka-connect kibana; do
  PID=$(podman inspect --format '{{.State.Pid}}' $name)
  echo "$name → PID $PID"
done
```

```bash
# Record 30s CPU profile per container — repeat for each
KAFKA_PID=$(podman inspect --format '{{.State.Pid}}' kafka1)
ES_PID=$(podman inspect --format '{{.State.Pid}}' es01)
FB_PID=$(podman inspect --format '{{.State.Pid}}' fluent-bit)

# Kafka1
sudo perf record -F 99 -p $KAFKA_PID -g --call-graph dwarf \
  -o ~/baseline/perf-kafka1.data -- sleep 30
sudo perf report -i ~/baseline/perf-kafka1.data \
  --stdio > ~/baseline/perf-kafka1-report.txt

# Elasticsearch
sudo perf record -F 99 -p $ES_PID -g --call-graph dwarf \
  -o ~/baseline/perf-es01.data -- sleep 30
sudo perf report -i ~/baseline/perf-es01.data \
  --stdio > ~/baseline/perf-es01-report.txt

# Fluent-bit
sudo perf record -F 99 -p $FB_PID -g --call-graph dwarf \
  -o ~/baseline/perf-fluent-bit.data -- sleep 30
sudo perf report -i ~/baseline/perf-fluent-bit.data \
  --stdio > ~/baseline/perf-fluent-bit-report.txt
```

> **JVM note:** For `es01` and `kafka1` (JVM processes), perf symbols will be mangled. Add `-XX:+PreserveFramePointer` to their JVM opts for readable stacks. Otherwise use async-profiler instead.

---

## 3. Scheduler & Latency (`perf sched`)

Capture scheduling latency — critical for understanding pipeline lag under attack.

```bash
# Record scheduler events for 30s
sudo perf sched record -o ~/baseline/perf-sched.data -- sleep 30

# Generate latency summary
sudo perf sched latency -i ~/baseline/perf-sched.data \
  2>&1 | tee ~/baseline/perf-sched-latency.txt

# Per-task wait/sleep breakdown
sudo perf sched timehist -i ~/baseline/perf-sched.data \
  2>&1 | head -100 | tee ~/baseline/perf-sched-timehist.txt
```

**Key metrics to baseline:**
- Max scheduling delay per container process
- Average wait time

---

## 4. Network I/O Baseline (`bpftrace`)

### 4a. TCP connection rate per container

```bash
sudo bpftrace -e '
kprobe:tcp_connect {
  @connects[comm] = count();
}
interval:s:30 {
  print(@connects);
  clear(@connects);
  exit();
}' 2>&1 | tee ~/baseline/bpf-tcp-connects.txt
```

### 4b. TCP send/receive throughput per process

```bash
sudo bpftrace -e '
kprobe:tcp_sendmsg {
  @send_bytes[comm] = sum(arg2);
}
kprobe:tcp_recvmsg {
  @recv_bytes[comm] = sum(arg2);
}
interval:s:30 {
  print(@send_bytes);
  print(@recv_bytes);
  clear(@send_bytes);
  clear(@recv_bytes);
  exit();
}' 2>&1 | tee ~/baseline/bpf-tcp-throughput.txt
```

### 4c. DNS lookups (inter-container name resolution)

```bash
sudo bpftrace -e '
kprobe:udp_sendmsg /comm != "sshd"/ {
  @dns_sends[comm] = count();
}
interval:s:30 {
  print(@dns_sends);
  exit();
}' 2>&1 | tee ~/baseline/bpf-dns.txt
```

---

## 5. Disk I/O Baseline (`bpftrace`)

### 5a. Block I/O latency per process

```bash
sudo bpftrace -e '
tracepoint:block:block_rq_issue {
  @start[args->dev, args->sector] = nsecs;
}
tracepoint:block:block_rq_complete
/@start[args->dev, args->sector]/ {
  @io_lat_us = hist((nsecs - @start[args->dev, args->sector]) / 1000);
  @_del = delete(@start[args->dev, args->sector]);
}
interval:s:30 {
  print(@io_lat_us);
  clear(@_del);
  exit();
}' 2>&1 | tee ~/baseline/bpf-blk-latency.txt
```

### 5b. File open patterns per container

```bash
sudo bpftrace -e '
tracepoint:syscalls:sys_enter_openat {
  @opens[comm] = count();
}
interval:s:30 {
  print(@opens);
  exit();
}' 2>&1 | tee ~/baseline/bpf-file-opens.txt
```

---

## 6. Memory Baseline (`bpftrace` + `/proc`)

### 6a. Page fault rate per process

```bash
sudo bpftrace -e '
software:page-faults:1 {
  @faults[comm] = count();
}
interval:s:30 {
  print(@faults);
  exit();
}' 2>&1 | tee ~/baseline/bpf-page-faults.txt
```

### 6b. RSS snapshot per container

```bash
for name in kafka1 es01 fluent-bit kafka-connect kibana grafana; do
  PID=$(docker inspect --format '{{.State.Pid}}' $name 2>/dev/null)
  if [ -n "$PID" ]; then
    RSS=$(awk '/VmRSS/{print $2}' /proc/$PID/status 2>/dev/null)
    echo "$name (pid $PID): RSS=${RSS}kB"
  fi
done 2>&1 | tee ~/baseline/mem-rss-snapshot.txt
```

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
}'
```

---

## 7. Kafka Pipeline Throughput Baseline

```bash
# Messages in/out per topic over 60s
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --all-groups \
  2>&1 | tee ~/baseline/kafka-consumer-lag-baseline.txt

# Topic offsets snapshot
docker exec kafka1 kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --time -1 \
  2>&1 | tee ~/baseline/kafka-offsets-baseline.txt
```

---

## 8. ftrace — Kernel Function Latency

Useful for tracing specific kernel paths (syscall overhead, scheduler functions).

```bash
# Mount debugfs if not already
sudo mount -t debugfs none /sys/kernel/debug 2>/dev/null || true

# Trace context switch latency for container PIDs
FB_PID=$(podman inspect --format '{{.State.Pid}}' fluent-bit)

sudo trace-cmd record \
  -e sched:sched_switch \
  -e sched:sched_wakeup \
  -P $FB_PID \
  sleep 30

sudo trace-cmd report 2>&1 \
  | head -200 \
  | tee ~/baseline/ftrace-fluent-bit-sched.txt
```

---

## 9. Snapshot Summary

Run this last to capture point-in-time system state:

```bash
cat > ~/baseline/snapshot.sh << 'EOF'
#!/bin/bash
OUT=~/baseline/system-snapshot.txt
echo "=== DATE ===" > $OUT && date >> $OUT
echo "=== UPTIME ===" >> $OUT && uptime >> $OUT
echo "=== VMSTAT ===" >> $OUT && vmstat 1 5 >> $OUT
echo "=== IOSTAT ===" >> $OUT && iostat -x 1 3 >> $OUT
echo "=== FREE ===" >> $OUT && free -h >> $OUT
echo "=== NETSTAT SUMMARY ===" >> $OUT && ss -s >> $OUT
echo "=== PODMAN STATS ===" >> $OUT
podman stats --no-stream \
  --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}" >> $OUT
echo "=== ES CLUSTER HEALTH ===" >> $OUT
curl -s http://localhost:9200/_cluster/health | python3 -m json.tool >> $OUT
echo "=== ES INDEX STATS ===" >> $OUT
curl -s http://localhost:9200/_cat/indices?v >> $OUT
echo "=== KAFKA TOPICS ===" >> $OUT
podman exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list >> $OUT
EOF

chmod +x ~/baseline/snapshot.sh
~/baseline/snapshot.sh
```

---

## 10. Baseline Archive

```bash
# Timestamp and archive everything
STAMP=$(date +%Y%m%d_%H%M%S)
tar -czf ~/baseline-${STAMP}.tar.gz ~/baseline/
echo "Baseline archived: ~/baseline-${STAMP}.tar.gz"
ls -lh ~/baseline-${STAMP}.tar.gz
```

---

## Post-Baseline Checklist

Before starting the attack profile, confirm you have captured:

| Artifact | File | Status |
|---|---|---|
| CPU counters | `perf-stat-baseline.txt` | ☐ |
| Kafka CPU profile | `perf-kafka1-report.txt` | ☐ |
| ES CPU profile | `perf-es01-report.txt` | ☐ |
| Fluent-bit CPU profile | `perf-fluent-bit-report.txt` | ☐ |
| Scheduler latency | `perf-sched-latency.txt` | ☐ |
| TCP connections | `bpf-tcp-connects.txt` | ☐ |
| TCP throughput | `bpf-tcp-throughput.txt` | ☐ |
| Block I/O latency | `bpf-blk-latency.txt` | ☐ |
| Page fault rate | `bpf-page-faults.txt` | ☐ |
| Memory RSS | `mem-rss-snapshot.txt` | ☐ |
| Kafka consumer lag | `kafka-consumer-lag-baseline.txt` | ☐ |
| System snapshot | `system-snapshot.txt` | ☐ |

Once all green — start the attack profile and re-run sections 1, 2, 4, 5, 6, 7, and 9 with output to `~/attack/` for diff comparison.

---

## Quick Diff After Attack

```bash
# Example: compare TCP connection rates
diff ~/baseline/bpf-tcp-connects.txt ~/attack/bpf-tcp-connects.txt

# Compare consumer lag (pipeline backpressure)
diff ~/baseline/kafka-consumer-lag-baseline.txt ~/attack/kafka-consumer-lag-baseline.txt

# Compare RSS growth
diff ~/baseline/mem-rss-snapshot.txt ~/attack/mem-rss-snapshot.txt
```
