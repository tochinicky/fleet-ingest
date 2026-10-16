#!/usr/bin/env bash
# The production profile under a VM-sized cap (decisions.md S8-6). Run on the lab VM, never on the target VM:
#   scripts/sizing/run.sh <cpus> <memory> <out-dir>      e.g. scripts/sizing/run.sh 1 2G ~/runs/sizing-b1ms
# Phases, each sampled every 5 s (the slice's memory and CPU, OOM kills, per-container memory):
#   1. build: the Node image built from scratch by a BuildKit container inside the cap, while nothing else runs;
#   2. cold start: the stack up with the OS page cache dropped;
#   3. steady: 20 min with the real gateway and 10 simulated gateways sending;
#   4. redeploy: `up -d --build` with the image rebuilt from scratch while the stack keeps running;
#   5. a second cold start after `down`.
# Needs sudo for the slice (a local cgroup on this machine). Prints a summary; raw samples go to <out-dir>.
set -euo pipefail
cpus="${1:?cpus}"; memory="${2:?memory}"; out="${3:?out-dir}"
cd "$(dirname "$0")/../.."
mkdir -p "$out"
export API_TOKEN="${API_TOKEN:-sizing-token-0123456789}" GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-sizing-only}" GATEWAY_COUNT=10
export GATEWAY_REPO="${GATEWAY_REPO:-$HOME/home-energy-gateway}"
compose=(docker compose -f deploy/compose.yaml -f deploy/compose.production.yaml -f deploy/compose.with-gateway.yaml -f scripts/sizing/compose.sizing.yaml --profile sim --profile with-gateway)
slice=/sys/fs/cgroup/fleetsizing.slice

# The slice: hard memory cap, no swap, CPU quota of <cpus> whole CPUs.
sudo systemctl stop fleetsizing-hold.service 2>/dev/null || true
sudo tee /run/systemd/system/fleetsizing.slice > /dev/null <<UNIT
[Slice]
MemoryMax=$memory
MemorySwapMax=0
CPUQuota=$((cpus * 100))%
UNIT
sudo systemctl daemon-reload
# A slice's cgroup exists only while something runs in it: a placeholder holds it for the whole run.
sudo systemd-run --quiet --slice=fleetsizing.slice --unit=fleetsizing-hold sleep infinity
for _ in 1 2 3 4 5; do [[ -f "$slice/cpu.stat" ]] && break; sleep 1; done
[[ -f "$slice/cpu.stat" ]] || { echo "the slice's cgroup didn't appear at $slice" >&2; exit 1; }
# BuildKit in its own container inside the slice, driven as a remote builder: buildx's own docker-container driver
# ignored its cgroup-parent option on this machine, which would have built outside the cap.
docker buildx rm fleetsizing >/dev/null 2>&1 || true
docker rm -f fleetsizing-buildkit >/dev/null 2>&1 || true
docker run -d --privileged --name fleetsizing-buildkit --cgroup-parent fleetsizing.slice moby/buildkit:buildx-stable-1 >/dev/null
docker buildx create --name fleetsizing --driver remote docker-container://fleetsizing-buildkit >/dev/null

sampler() { # phase name; samples until the file "$out/.stop" appears
  local phase=$1 last_usage last_t
  rm -f "$out/.stop"
  last_usage=$(awk '/usage_usec/ {print $2}' "$slice/cpu.stat"); last_t=$(date +%s%N)
  while [[ ! -f "$out/.stop" ]]; do
    sleep 5
    local usage t mem
    usage=$(awk '/usage_usec/ {print $2}' "$slice/cpu.stat"); t=$(date +%s%N); mem=$(cat "$slice/memory.current")
    echo "$phase $(date -u +%T) mem_mb=$((mem / 1048576)) cpu_pct=$(( (usage - last_usage) * 100000 / ((t - last_t) / 1000) / 1000 )) oom=$(awk '/oom_kill/ {print $2}' "$slice/memory.events")" >> "$out/samples.txt"
    last_usage=$usage; last_t=$t
  done
}
phase() { # name, command...
  local name=$1; shift
  echo "== $name $(date -u +%T)" >> "$out/samples.txt"
  sampler "$name" & local sp=$!
  "$@" > "$out/$name.log" 2>&1 || echo "$name: FAILED (exit $?)" >> "$out/samples.txt"
  touch "$out/.stop"; wait "$sp" || true
}
# The one image the cloud services share, from scratch, built inside the cap (the step that OOMs small boxes).
build() { docker buildx build --builder fleetsizing --no-cache --load -t fleet-ingest/node:dev . ; }
redeploy() { build && "${compose[@]}" up -d && sleep 60; }
cold_start() { sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; "${compose[@]}" up -d && sleep 120; }

scripts/dev-certs.sh 10 >/dev/null
[[ -f deploy/certs/gw-001.crt ]] || scripts/pki.sh issue deploy/certs gw-001 >/dev/null
"${compose[@]}" down >/dev/null 2>&1 || true
# The load generators (the real gateway, its home, fleet-sim) are built normally, outside the cap.
"${compose[@]}" build gateway inverter-sunspec inverter-vendorb chargepoint battery meter toxiproxy-home > "$out/load-build.log" 2>&1
phase build build
phase cold-start cold_start
phase steady sleep 1200
# The health verdict over the steady phase. -G with --data-urlencode: a raw `[15m]` in the URL is a curl glob range,
# and curl failed silently on it in the first runs (S8-6).
promq() { curl -sfG "http://127.0.0.1:9090/api/v1/query" --data-urlencode "query=$1" > "$out/$2.json" || echo "$2: query failed" >> "$out/samples.txt"; }
promq 'histogram_quantile(0.95, sum(rate(ingest_lag_seconds_bucket[15m])) by (le))' lag-p95
promq 'sum(increase(fleet_health_job_pass_duration_seconds_count[15m])) - sum(increase(fleet_health_job_pass_duration_seconds_bucket{le="2"}[15m]))' passes-over-2s
promq 'sum(increase(ingest_readings_stored_total[15m]))' stored-15m
promq 'max(max_over_time(fleet_health_job_pass_seconds[15m]))' pass-max
phase redeploy redeploy
"${compose[@]}" ps --format '{{.Name}} {{.Status}}' > "$out/ps-after-redeploy.txt"
"${compose[@]}" down > /dev/null 2>&1
phase cold-start-2 cold_start
"${compose[@]}" ps --format '{{.Name}} {{.Status}}' > "$out/ps-final.txt"
docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' > "$out/containers.txt"
cat "$slice/memory.peak" > "$out/memory-peak.txt" 2>/dev/null || true
"${compose[@]}" down > /dev/null 2>&1
docker buildx rm fleetsizing >/dev/null 2>&1 || true
docker rm -f fleetsizing-buildkit >/dev/null 2>&1 || true
sudo systemctl stop fleetsizing-hold.service fleetsizing.slice; sudo rm -f /run/systemd/system/fleetsizing.slice; sudo systemctl daemon-reload
echo "done: $out"
