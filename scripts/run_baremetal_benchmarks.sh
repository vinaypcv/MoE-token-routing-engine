#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${IFACE:?Set IFACE to the disposable DUT NIC interface}"
: "${GENERATOR_HOST:?Set GENERATOR_HOST to a separate traffic-generator host}"
: "${GENERATOR_IFACE:?Set GENERATOR_IFACE on the peer host}"
: "${DUT_MAC:?Set DUT_MAC to the DUT NIC MAC address}"
: "${GENERATOR_SRC_IP:?Set GENERATOR_SRC_IP to the peer source IPv4 address}"
: "${DUT_DST_IP:?Set DUT_DST_IP to the DUT interface IPv4 address}"
: "${MOE_PTP_SYNC_CONFIRMED:?Set MOE_PTP_SYNC_CONFIRMED=1 only after verifying DUT and peer clocks are PTP synchronized}"
: "${MOE_PTP_UNCERTAINTY_NS:?Set measured maximum PTP clock uncertainty in nanoseconds}"
if [[ "${MOE_PTP_SYNC_CONFIRMED}" != 1 ]]; then
    echo "Cross-host T0/T1 requires PTP-synchronized CLOCK_TAI on the DUT and generator." >&2
    exit 1
fi
if [[ ! "${MOE_PTP_UNCERTAINTY_NS}" =~ ^[0-9]+$ ]]; then
    echo "MOE_PTP_UNCERTAINTY_NS must be a nonnegative integer." >&2
    exit 1
fi

queue_id="${QUEUE_ID:-0}"
run_seconds="${MOE_RAW_DURATION_SECONDS:-10}"
pps="${MOE_RAW_RATE_PPS:-10000}"
skew="${MOE_RAW_ZIPF_SKEW:-1.5}"
seed="${MOE_RAW_SEED:-42}"
source_port="${MOE_RAW_SOURCE_PORT:-41000}"
destination_port="${MOE_RAW_DESTINATION_PORT:-9000}"
experts="${MOE_RAW_EXPERTS:-8}"
feature_bytes="${MOE_RAW_FEATURE_BYTES:-64}"
metrics_addr="${MOE_METRICS_ADDR:-127.0.0.1:19810}"
metrics_url="http://${metrics_addr}/metrics"
peer_profiler="${MOE_PEER_RAW_PROFILER:-/tmp/moe-raw-packet-profiler}"
output_dir="${MOE_BENCH_OUTPUT_DIR:-${repo_root}/artifacts/af-xdp-$(date -u +%Y%m%d_%H%M%S)}"
object_path="${repo_root}/target/bpfel-unknown-none/release/libmoe_ebpf_kernel.so"
perf_pid=''
xdp_pid=''

if [[ ! "${run_seconds}" =~ ^[1-9][0-9]*$ || ! "${pps}" =~ ^[1-9][0-9]*$ ]]; then
    echo "MOE_RAW_DURATION_SECONDS and MOE_RAW_RATE_PPS must be positive integers." >&2
    exit 1
fi
if [[ ! "${queue_id}" =~ ^[0-9]+$ ]] || (( queue_id >= 64 )); then
    echo "QUEUE_ID must be a numeric XSK_MAP queue index in 0..63." >&2
    exit 1
fi
max_run_seconds="${MOE_MAX_RUN_SECONDS:-$((run_seconds + 30))}"
drain_seconds="${MOE_DRAIN_SECONDS:-3}"
if [[ ! "${max_run_seconds}" =~ ^[1-9][0-9]*$ || ! "${drain_seconds}" =~ ^[0-9]+$ ]]; then
    echo "MOE_MAX_RUN_SECONDS must be positive and MOE_DRAIN_SECONDS nonnegative." >&2
    exit 1
fi
if [[ "$(uname -r)" == *microsoft* || "${IFACE}" == lo || "${IFACE}" == docker* || "${IFACE}" == veth* ]]; then
    echo "Refusing likely loopback/virtual interface '${IFACE}'; select a disposable physical NIC on native Linux." >&2
    exit 1
fi

stop_loader() {
    if [[ -n "${xdp_pid}" ]] && kill -0 "${xdp_pid}" 2>/dev/null; then
        sudo -n kill -INT "${xdp_pid}" 2>/dev/null || true
    fi
    if [[ -n "${perf_pid}" ]]; then
        wait "${perf_pid}" 2>/dev/null || true
    fi
}
cleanup() {
    stop_loader
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cd "${repo_root}"
mkdir -p "${output_dir}"
command -v ethtool >/dev/null || { echo "ethtool is required" >&2; exit 1; }
command -v perf >/dev/null || { echo "perf is required" >&2; exit 1; }
command -v ssh >/dev/null || { echo "ssh is required to reach the separate generator host" >&2; exit 1; }
command -v scp >/dev/null || { echo "scp is required to install the raw profiler on the generator host" >&2; exit 1; }
sudo -v

echo "Building the pinned BPF object, AF_XDP loader, and raw packet profiler..."
cargo +nightly-2025-12-01 build -Z build-std=core --release -p moe-ebpf-kernel --target bpfel-unknown-none
cargo build --release --bin xdp_loader --bin raw_packet_profiler
if [[ "$(ssh "${GENERATOR_HOST}" uname -m)" != "$(uname -m)" ]]; then
    echo "DUT and generator architectures differ; build raw_packet_profiler for the peer and set MOE_PEER_RAW_PROFILER." >&2
    exit 1
fi
peer_profiler_dir="$(dirname "${peer_profiler}")"
printf -v peer_mkdir 'mkdir -p %q' "${peer_profiler_dir}"
ssh "${GENERATOR_HOST}" "${peer_mkdir}"
scp "${repo_root}/target/release/raw_packet_profiler" "${GENERATOR_HOST}:${peer_profiler}"
printf -v peer_chmod 'chmod 755 %q' "${peer_profiler}"
ssh "${GENERATOR_HOST}" "${peer_chmod}"

ip -details link show dev "${IFACE}" >"${output_dir}/interface-before.txt"
sudo ethtool -i "${IFACE}" >"${output_dir}/driver.txt"
sudo ethtool -l "${IFACE}" >"${output_dir}/channels.txt" 2>&1 || true
sudo ethtool -S "${IFACE}" >"${output_dir}/nic-stats-before.txt"
echo "Ensure the generator's fixed UDP 5-tuple is steered to RX queue ${queue_id}; this harness does not configure NIC RSS/ntuple rules."

printf '{\n  "schema_version": 1,\n  "mode": "native_af_xdp_hardware",\n  "dut_host": "%s",\n  "dut_interface": "%s",\n  "queue_id": %s,\n  "generator_host": "%s",\n  "duration_seconds": %s,\n  "target_rate_pps": %s,\n  "zipf_skew": %s,\n  "seed": %s,\n  "timestamp": "CLOCK_MONOTONIC in first 8 feature bytes",\n  "gpu_direct": "not_measured"\n}\n' \
    "$(hostname)" "${IFACE}" "${queue_id}" "${GENERATOR_HOST}" \
    "${run_seconds}" "${pps}" "${skew}" "${seed}" >"${output_dir}/run_metadata.json"
python3 - "${output_dir}/run_metadata.json" "${MOE_PTP_UNCERTAINTY_NS}" <<'PY'
import json
import sys

path, uncertainty = sys.argv[1:]
with open(path, encoding="utf-8") as source:
    metadata = json.load(source)
metadata["timestamp"] = "PTP-synchronized CLOCK_TAI in first 8 feature bytes"
metadata["ptp_max_uncertainty_ns"] = int(uncertainty)
with open(path, "w", encoding="utf-8") as output:
    json.dump(metadata, output, indent=2)
    output.write("\n")
PY

if [[ ! -x "${repo_root}/target/release/xdp_loader" || ! -f "${object_path}" ]]; then
    echo "Build the release xdp_loader and BPF object before running this harness." >&2
    exit 1
fi
printf -v peer_test 'test -x %q' "${peer_profiler}"
if ssh "${GENERATOR_HOST}" "${peer_test}"; then
    :
else
    echo "Raw profiler not found/executable on generator peer: ${peer_profiler}; copy the release binary there." >&2
    exit 1
fi
if pgrep -x xdp_loader >/dev/null 2>&1; then
    echo "An xdp_loader process is already running; stop it before starting a measured run." >&2
    exit 1
fi

if curl --silent --fail --max-time 1 "${metrics_url}" >/dev/null 2>&1; then
    echo "Metrics endpoint ${metrics_url} is already in use; choose another MOE_METRICS_ADDR." >&2
    exit 1
fi

export MOE_METRICS_ADDR="${metrics_addr}"
export MOE_EMBED_T0=true
export MOE_RAW_EXPERTS="${experts}"
export MOE_RAW_FEATURE_BYTES="${feature_bytes}"
export MOE_T0_CLOCK=tai

echo "Starting XDP_ZEROCOPY AF_XDP loader on ${IFACE} queue ${queue_id}..."
timeout --signal=TERM --kill-after=5s "${max_run_seconds}s" \
    sudo -n perf stat -o "${output_dir}/perf-stat.txt" \
    -e cycles,instructions,cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses,context-switches \
    -- "${repo_root}/target/release/xdp_loader" "${IFACE}" "${queue_id}" "${object_path}" \
    >"${output_dir}/xdp-loader.log" 2>&1 &
perf_pid=$!

ready=0
for _ in {1..100}; do
    xdp_pid="$(pgrep -n -x xdp_loader || true)"
    if [[ -n "${xdp_pid}" ]] && curl --silent --fail --max-time 1 "${metrics_url}" >/dev/null 2>&1; then
        ready=1
        break
    fi
    if ! kill -0 "${perf_pid}" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if [[ "${ready}" != 1 ]]; then
    cat "${output_dir}/xdp-loader.log" >&2
    echo "AF_XDP loader failed to attach or expose metrics; no traffic was generated." >&2
    exit 1
fi
if ! grep -q "Zero-copy AF_XDP redirect attached" "${output_dir}/xdp-loader.log"; then
    echo "Loader did not report its XDP_ZEROCOPY attachment; refusing benchmark." >&2
    exit 1
fi
curl --silent --show-error --fail --max-time 2 "${metrics_url}" >"${output_dir}/metrics-before.txt"

printf -v remote_command '%q ' "${peer_profiler}" "${GENERATOR_IFACE}" "${DUT_MAC}" "${GENERATOR_SRC_IP}" "${DUT_DST_IP}" "${source_port}" "${destination_port}" "${run_seconds}" "${pps}" "${skew}" "${seed}"
echo "Sending raw Ethernet Zipf traffic from ${GENERATOR_HOST}..."
timeout --signal=TERM --kill-after=5s "$((run_seconds + 15))s" \
    ssh "${GENERATOR_HOST}" \
    "MOE_RAW_EXPERTS=${experts} MOE_RAW_FEATURE_BYTES=${feature_bytes} MOE_T0_CLOCK=tai ${remote_command}" \
    >"${output_dir}/raw-generator.log" 2>&1

if (( drain_seconds > 0 )); then
    sleep "${drain_seconds}"
fi
curl --silent --show-error --fail --max-time 2 "${metrics_url}" >"${output_dir}/metrics-after.txt"
sudo ethtool -S "${IFACE}" >"${output_dir}/nic-stats-after.txt"
python3 - "${output_dir}/metrics-before.txt" "${output_dir}/metrics-after.txt" \
    "${output_dir}/raw-generator.log" "${output_dir}/counter-reconciliation.json" <<'PY'
import json
import re
import sys


def load_metrics(path):
    values = {}
    for line in open(path, encoding="utf-8"):
        if not line or line.startswith("#"):
            continue
        name, _, value = line.partition(" ")
        try:
            values[name] = float(value)
        except ValueError:
            continue
    return values


before = load_metrics(sys.argv[1])
after = load_metrics(sys.argv[2])
generator_text = open(sys.argv[3], encoding="utf-8").read()
sent_match = re.search(r"raw Ethernet frames sent:\s*(\d+)", generator_text)
if sent_match is None:
    raise SystemExit("Could not parse the peer generator's sent-frame count")
sent = int(sent_match.group(1))


def delta(name):
    return max(0, int(after.get(name, 0) - before.get(name, 0)))


received = delta("moe_rx_packets_total")
dispatched = delta("moe_dispatched_total")
processed = delta("moe_processed_jobs_total")
application_drops = sum(
    delta(name)
    for name in (
        "moe_saturated_drops_total",
        "moe_invalid_packets_total",
        "moe_closed_queue_drops_total",
    )
)
summary = {
    "sent_packets": sent,
    "af_xdp_received_packets": received,
    "dispatched_jobs": dispatched,
    "processed_jobs": processed,
    "application_drops": application_drops,
    "unreceived_packets": max(0, sent - received),
    "all_sent_reconciled_no_application_drops": sent == received and application_drops == 0,
    "fully_processed_at_snapshot": processed == dispatched,
    "execution_latency_samples": delta("moe_ingress_to_completion_latency_seconds_count"),
    "execution_p50_ns": None,
    "execution_p99_ns": None,
    "execution_p99_9_ns": None,
    "execution_timestamp_errors": delta("moe_execution_timestamp_errors_total"),
    "ptp_max_uncertainty_ns": int(__import__("os").environ["MOE_PTP_UNCERTAINTY_NS"]),
    "independent_xdp_redirect_counter": None,
    "note": "AF_XDP RX is not an independent eBPF redirect counter; inspect NIC snapshots separately.",
}

bucket_prefix = 'moe_ingress_to_completion_latency_seconds_bucket{le="'
execution_buckets = []
for name, count_after in after.items():
    if not name.startswith(bucket_prefix):
        continue
    label = name[len(bucket_prefix):].split('"}', 1)[0]
    count_delta = max(0, int(count_after - before.get(name, 0.0)))
    upper_bound_ns = None if label == "+Inf" else int(float(label) * 1_000_000_000)
    execution_buckets.append((upper_bound_ns, count_delta))

for percentile, field in (
    (0.50, "execution_p50_ns"),
    (0.99, "execution_p99_ns"),
    (0.999, "execution_p99_9_ns"),
):
    rank = max(1, int(summary["execution_latency_samples"] * percentile + 0.999999))
    for upper_bound_ns, cumulative_count in execution_buckets:
        if cumulative_count >= rank:
            summary[field] = upper_bound_ns
            break

with open(sys.argv[4], "w", encoding="utf-8") as output:
    json.dump(summary, output, indent=2)
    output.write("\n")
print(json.dumps(summary, indent=2))
PY
stop_loader
perf_pid=''
xdp_pid=''
cat "${output_dir}/raw-generator.log"
cat "${output_dir}/perf-stat.txt"
echo "Captured evidence: ${output_dir}"
echo "AF_XDP RX/dispatched/processed metrics are in metrics-after.txt; NIC counters are before/after snapshots."
echo "This runner requests XDP_ZEROCOPY and requires the loader's attach message; independently inspect driver-specific counters before claiming NIC zero-copy or lossless delivery."
echo "DMA-BUF/GPUDirect and zero-copy into GPU memory are not measured by this harness."
