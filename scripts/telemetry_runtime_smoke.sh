#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
metrics_addr="${MOE_METRICS_ADDR:-127.0.0.1:19100}"
udp_addr="${MOE_DEMO_UDP_ADDR:-127.0.0.1:19000}"
metrics_url="http://${metrics_addr}/metrics"
log_file="${TMPDIR:-/tmp}/moe-telemetry-runtime-smoke.log"
demo_pid=''

cleanup() {
    if [[ -n "${demo_pid}" ]]; then
        kill -TERM "${demo_pid}" 2>/dev/null || true
        wait "${demo_pid}" 2>/dev/null || true
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cd "${repo_root}"
cargo build --release --bin telemetry_demo --bin traffic_profiler

if curl --silent --fail --max-time 1 "${metrics_url}" >/dev/null 2>&1; then
    echo "Metrics endpoint ${metrics_url} is already in use; choose another MOE_METRICS_ADDR." >&2
    exit 1
fi

./target/release/telemetry_demo >"${log_file}" 2>&1 &
demo_pid=$!

ready=0
for _ in $(seq 1 50); do
    if curl --silent --fail --max-time 1 "${metrics_url}" >/dev/null 2>&1; then
        ready=1
        break
    fi
    if ! kill -0 "${demo_pid}" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if [[ "${ready}" != 1 ]]; then
    echo "Telemetry demo failed to become ready; log follows: ${log_file}" >&2
    cat "${log_file}" >&2
    exit 1
fi

timeout --signal=TERM --kill-after=2s 15s \
    ./target/release/traffic_profiler "${udp_addr}" 1 1 8 uniform 70 100 16

metrics="$(curl --silent --show-error --fail --max-time 2 "${metrics_url}")"
metric_value() {
    awk -v name="$1" '$1 == name { print $2; found = 1; exit } END { if (!found) exit 1 }' <<<"${metrics}"
}

rx_packets="$(metric_value moe_rx_packets_total)"
dispatched="$(metric_value moe_dispatched_total)"
processed="$(metric_value moe_processed_jobs_total)"
if (( rx_packets == 0 || dispatched == 0 || processed == 0 )); then
    echo "Runtime counters did not advance: rx=${rx_packets}, dispatched=${dispatched}, processed=${processed}" >&2
    exit 1
fi

echo "UDP-to-metrics smoke passed: rx=${rx_packets}, dispatched=${dispatched}, processed=${processed}."
echo "This validates the simulation path only; it does not validate AF_XDP attachment or NIC zero-copy."