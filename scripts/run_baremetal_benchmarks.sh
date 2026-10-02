#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
timestamp="$(date -u +%Y%m%d_%H%M%S)"
output_dir="${MOE_BENCH_OUTPUT_DIR:-${repo_root}/artifacts/benchmarks/${timestamp}}"
metrics_addr="${MOE_METRICS_ADDR:-127.0.0.1:19810}"
udp_addr="${MOE_DEMO_UDP_ADDR:-127.0.0.1:19800}"
metrics_url="http://${metrics_addr}/metrics"
queue_capacity="${MOE_DEMO_QUEUE_CAPACITY:-256}"
udp_baseline_seconds="${MOE_UDP_BASELINE_SECONDS:-2}"
zipf_duration_seconds="${MOE_ZIPF_DURATION_SECONDS:-2}"
demo_pid=''

if [[ ! "${udp_baseline_seconds}" =~ ^[1-9][0-9]*$ || ! "${zipf_duration_seconds}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Benchmark durations must be positive integer seconds." >&2
    exit 1
fi

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
mkdir -p "${output_dir}"

echo "Building benchmark binaries..."
cargo build --release --bin bench_ingestion --bin telemetry_demo --bin traffic_profiler

if curl --silent --fail --max-time 1 "${metrics_url}" >/dev/null 2>&1; then
    echo "Metrics endpoint ${metrics_url} is already in use; choose another MOE_METRICS_ADDR." >&2
    exit 1
fi

cat >"${output_dir}/run_metadata.json" <<EOF
{
  "schema_version": 1,
  "run_utc": "${timestamp}",
  "host": "$(hostname)",
  "kernel": "$(uname -sr)",
  "mode": "udp_loopback_and_synthetic_moe",
  "af_xdp_zero_copy": "not_measured_by_this_harness"
}
EOF

echo "Running standard UDP recvmmsg loopback baseline..."
timeout --signal=TERM --kill-after=2s "$((udp_baseline_seconds + 10))s" \
    ./target/release/bench_ingestion "${MOE_BASELINE_ADDR:-127.0.0.1:19700}" \
    "${udp_baseline_seconds}" \
    >"${output_dir}/standard_udp_baseline.txt" 2>&1

export MOE_METRICS_ADDR="${metrics_addr}"
export MOE_DEMO_UDP_ADDR="${udp_addr}"
export MOE_DEMO_QUEUE_CAPACITY="${queue_capacity}"
export MOE_EMBED_T0=true
./target/release/telemetry_demo >"${output_dir}/telemetry_demo.log" 2>&1 &
demo_pid=$!

ready=0
for _ in {1..50}; do
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
    cat "${output_dir}/telemetry_demo.log" >&2
    echo "Telemetry demo failed to become ready at ${metrics_url}." >&2
    exit 1
fi

echo "Running seeded Zipf workload through the synthetic MoE consumer..."
export MOE_PROFILE_JSON="${output_dir}/zipf_skew_1.5_run.json"
export MOE_METRICS_URL="${metrics_url}"
export MOE_DEMO_QUEUE_CAPACITY="${queue_capacity}"
timeout --signal=TERM --kill-after=2s "$((zipf_duration_seconds + 10))s" \
    ./target/release/traffic_profiler "${udp_addr}" \
    "${MOE_ZIPF_WORKERS:-1}" \
    "${zipf_duration_seconds}" \
    "${MOE_ZIPF_EXPERTS:-8}" zipf 70 \
    "${MOE_ZIPF_RATE_PPS:-20000}" \
    "${MOE_FEATURE_BYTES:-64}" \
    "${MOE_ZIPF_SKEW:-1.5}" \
    "${MOE_ZIPF_SEED:-42}" \
    >"${output_dir}/zipf_profiler.txt" 2>&1

cat "${output_dir}/standard_udp_baseline.txt"
cat "${output_dir}/zipf_profiler.txt"
echo "Reports and logs: ${output_dir}"
echo "This harness measures UDP loopback and synthetic MoE processing; it does not run AF_XDP, NIC line-rate, or hardware perf tests."