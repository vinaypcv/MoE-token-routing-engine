#!/usr/bin/env bash
set -Eeuo pipefail

BOLD='\033[1;32m'
WARN='\033[1;33m'
NC='\033[0m'
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${TMPDIR:-/tmp}/moe-traffic-storm"
METRICS_ADDR="${MOE_METRICS_ADDR:-0.0.0.0:9100}"
CLIENT_METRICS_ADDR="${MOE_METRICS_CLIENT_ADDR:-${METRICS_ADDR}}"
if [[ "${CLIENT_METRICS_ADDR}" == 0.0.0.0:* ]]; then
    CLIENT_METRICS_ADDR="127.0.0.1:${CLIENT_METRICS_ADDR##*:}"
fi
DASHBOARD_URL="${MOE_DASHBOARD_URL:-http://${CLIENT_METRICS_ADDR}/}"
METRICS_PROBE_URL="${MOE_METRICS_PROBE_URL:-http://${CLIENT_METRICS_ADDR}/metrics}"
UDP_ADDR="${MOE_DEMO_UDP_ADDR:-127.0.0.1:9000}"
WORKERS="${WORKERS:-4}"
EXPERTS="${EXPERTS:-8}"
DURATION="${DURATION_SECONDS:-10}"
RATE="${TOTAL_PPS:-40000}"
FEATURE_BYTES="${FEATURE_BYTES:-256}"
QUEUE_CAPACITY="${QUEUE_CAPACITY:-128}"
SERVICE_RATE="${SERVICE_PER_EXPERT_PER_100MS:-8}"
DEMO_PID=''

cleanup() {
    if [[ -n "${DEMO_PID}" ]]; then
        kill -INT "${DEMO_PID}" 2>/dev/null || true
        wait "${DEMO_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

need_command() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "Required command not found: $1" >&2
        exit 1
    }
}

need_command cargo
need_command curl
mkdir -p "${LOG_DIR}"
cd "${ROOT_DIR}"

echo -e "${BOLD}===============================================================${NC}"
echo -e "${BOLD} AF_XDP MoE Engine: Live Traffic Storm & Hot-Spot Visualizer ${NC}"
echo -e "${BOLD}===============================================================${NC}"

echo -e "\n${BOLD}[1/4] Building traffic and telemetry binaries...${NC}"
cargo build --release --bin telemetry_demo --bin traffic_profiler

if curl --silent --fail "${METRICS_PROBE_URL}" >/dev/null 2>&1; then
    echo "Metrics endpoint ${METRICS_ADDR} is already in use; stop it or choose another MOE_METRICS_ADDR." >&2
    exit 1
fi

export MOE_METRICS_ADDR="${METRICS_ADDR}"
export MOE_DEMO_UDP_ADDR="${UDP_ADDR}"
export MOE_DEMO_QUEUE_CAPACITY="${QUEUE_CAPACITY}"
export MOE_DEMO_SERVICE_PER_TICK="${SERVICE_RATE}"

echo -e "\n${BOLD}[2/4] Starting traffic-driven telemetry preview...${NC}"
./target/release/telemetry_demo >"${LOG_DIR}/telemetry.log" 2>&1 &
DEMO_PID=$!

ready=0
for _ in $(seq 1 100); do
    if curl --silent --fail "${METRICS_PROBE_URL}" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 0.1
done
if [[ "${ready}" != 1 ]]; then
    echo "Telemetry demo failed to start; see ${LOG_DIR}/telemetry.log" >&2
    exit 1
fi

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo -e "\n${BOLD}[3/4] Starting Prometheus and Grafana...${NC}"
    docker compose -f monitoring/compose.yml up -d --wait
else
    echo -e "\n${WARN}[3/4] Docker daemon unavailable; using the built-in dashboard only.${NC}"
fi

echo -e "\n${BOLD}[4/4] Running uniform then 80% Expert 0 hot-spot traffic...${NC}"
./target/release/traffic_profiler "${UDP_ADDR}" "${WORKERS}" "${DURATION}" "${EXPERTS}" uniform 70 "${RATE}" "${FEATURE_BYTES}"
./target/release/traffic_profiler "${UDP_ADDR}" "${WORKERS}" "${DURATION}" "${EXPERTS}" skewed 80 "${RATE}" "${FEATURE_BYTES}"

cat <<EOF

Traffic storm finished.
Live dashboard:  ${DASHBOARD_URL}
Prometheus data: ${METRICS_PROBE_URL}
Grafana:         http://localhost:3000/ (admin / moe-demo-local; when Docker is running)
Grafana folder:  MoE / AF_XDP Mixture-of-Experts Kernel Bypass Telemetry
Demo logs:       ${LOG_DIR}

The demo consumes loopback UDP and simulates bounded expert service. It is not an XDP attachment or NIC zero-copy benchmark.
EOF

if [[ -t 0 ]]; then
    read -r -p "Press Enter to stop the live telemetry preview... " _
fi
