#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
metrics_addr="${MOE_METRICS_ADDR:-0.0.0.0:9100}"
udp_addr="${MOE_DEMO_UDP_ADDR:-127.0.0.1:9000}"
workers="${WORKERS:-4}"
experts="${EXPERTS:-8}"
duration="${DURATION_SECONDS:-10}"
rate="${TOTAL_PPS:-40000}"
features="${FEATURE_BYTES:-256}"
queue_capacity="${QUEUE_CAPACITY:-128}"
service_rate="${SERVICE_PER_EXPERT_PER_100MS:-8}"
log_dir="${TMPDIR:-/tmp}/moe-traffic-storm"
mkdir -p "$log_dir"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to start Prometheus and Grafana." >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon is unavailable. Start Docker Desktop/the Docker service, then rerun." >&2
    exit 1
fi
for tool in cargo curl; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "Required command not found: $tool" >&2
        exit 1
    fi
done

cd "$repo_root"
cargo build --release --bin telemetry_demo --bin bench_ingestion --bin traffic_profiler

if curl --silent --fail "http://127.0.0.1:9100/metrics" >/dev/null 2>&1; then
    echo "Port 9100 already serves metrics; stop that process or set a different MOE_METRICS_ADDR." >&2
    exit 1
fi

export MOE_METRICS_ADDR="$metrics_addr"
export MOE_DEMO_UDP_ADDR="$udp_addr"
export MOE_DEMO_QUEUE_CAPACITY="$queue_capacity"
export MOE_DEMO_SERVICE_PER_TICK="$service_rate"
./target/release/telemetry_demo >"$log_dir/telemetry.log" 2>&1 &
telemetry_pid=$!
echo "$telemetry_pid" >"$log_dir/telemetry.pid"

receiver_seconds=$((duration * 2 + 10))
./target/release/bench_ingestion "$udp_addr" "$receiver_seconds" --external >"$log_dir/receiver.log" 2>&1 &
receiver_pid=$!
cleanup() {
    kill "$receiver_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

ready=0
for _ in $(seq 1 100); do
    if curl --silent --fail "http://127.0.0.1:9100/metrics" >/dev/null 2>&1 && grep -q "Listening on" "$log_dir/receiver.log"; then
        ready=1
        break
    fi
    sleep 0.1
done
if [[ "$ready" != 1 ]]; then
    echo "Telemetry or UDP receiver did not become ready. Logs: $log_dir" >&2
    exit 1
fi

docker compose -f monitoring/compose.yml up -d --wait

send_phase() {
    local mode="$1"
    echo "Starting $mode traffic: workers=$workers duration=${duration}s target=${rate}pps"
    ./target/release/traffic_profiler \
        "$udp_addr" "$workers" "$duration" "$experts" "$mode" 70 "$rate" "$features"
}

send_phase uniform
send_phase skewed

cat <<EOF

Traffic storm complete. The synthetic telemetry process remains running (PID $telemetry_pid).
Grafana:     http://localhost:3000/ (admin / moe-demo-local)
Prometheus:  http://localhost:9090/
Dashboard:   MoE folder -> MoE AF_XDP Pipeline
Metrics:     http://localhost:9100/metrics
Logs:        $log_dir

This demo models bounded worker queues from real loopback UDP input. It is not an AF_XDP/NIC zero-copy measurement.
Stop the preview process with: kill $telemetry_pid
Stop monitoring containers with: docker compose -f monitoring/compose.yml down
EOF
