#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_addr="${1:-127.0.0.1:9000}"
workers="${2:-4}"
duration="${3:-30}"
experts="${4:-8}"
total_pps="${5:-40000}"
feature_bytes="${6:-256}"
output_file="${7:-benchmark-report.csv}"
metadata_file="${output_file%.csv}.json"
profiler="${TRAFFIC_PROFILER:-$repo_root/target/release/traffic_profiler}"

if [[ ! -x "$profiler" ]]; then
    echo "traffic_profiler not found or not executable: $profiler" >&2
    exit 1
fi

mkdir -p "$(dirname "$output_file")"
printf 'mode,workers,duration_seconds,experts,total_pps,feature_bytes,packets_sent,payload_mib,average_rate,average_data_gbit\n' >"$output_file"
git_revision="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo unknown)"
kernel="$(uname -sr 2>/dev/null || echo unknown)"
timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cat >"$metadata_file" <<EOF
{
    "timestamp_utc": "$timestamp",
    "git_revision": "$git_revision",
    "kernel": "$kernel",
    "target_addr": "$target_addr",
    "workers": $workers,
    "duration_seconds": $duration,
    "experts": $experts,
    "total_pps": $total_pps,
    "feature_bytes": $feature_bytes,
    "csv_report": "$(basename "$output_file")"
}
EOF

run_phase() {
    local mode="$1"
    local log_file
    log_file="$(mktemp)"
    trap 'rm -f "$log_file"' RETURN

    "$profiler" "$target_addr" "$workers" "$duration" "$experts" "$mode" 70 "$total_pps" "$feature_bytes" | tee "$log_file"

    local packets payload rate data
    packets="$(sed -n 's/^Packets sent : //p' "$log_file")"
    payload="$(sed -n 's/^Payload MiB  : //p' "$log_file")"
    rate="$(sed -n 's/^Average rate : //p' "$log_file")"
    data="$(sed -n 's/^Average data : //p' "$log_file")"
    if [[ -z "$packets" || -z "$rate" ]]; then
        echo "Could not parse profiler output for $mode phase" >&2
        exit 1
    fi
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$mode" "$workers" "$duration" "$experts" "$total_pps" "$feature_bytes" \
        "$packets" "$payload" "$rate" "$data" >>"$output_file"
}

echo "Writing reproducible traffic report to $output_file"
run_phase uniform
run_phase skewed
echo "Benchmark report complete: $output_file"
echo "Benchmark metadata: $metadata_file"