#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
duration="${1:-30}"
output_dir="${2:-benchmark-matrix-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$output_dir"

git_revision="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo unknown)"
cat >"$output_dir/manifest.json" <<EOF
{
  "git_revision": "$git_revision",
  "timestamp_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "duration_seconds": $duration,
  "comparisons": [
    {"name": "standard_udp", "status": "run_by_bench_ingestion"},
    {"name": "simulation", "status": "run_by_traffic_profiler"},
    {"name": "af_xdp_copy", "status": "requires_native_linux_nic"},
    {"name": "af_xdp_zero_copy", "status": "requires_native_linux_nic"}
  ]
}
EOF

if [[ ! -x "$repo_root/target/release/bench_ingestion" || ! -x "$repo_root/target/release/traffic_profiler" ]]; then
    echo "Build release binaries before running the matrix." >&2
    exit 1
fi

echo "Running standard UDP baseline"
"$repo_root/target/release/bench_ingestion" 127.0.0.1 "$duration" >"$output_dir/standard_udp.txt" 2>&1

echo "Running uniform simulated pipeline phase"
"$repo_root/target/release/traffic_profiler" 127.0.0.1:9000 4 "$duration" 8 uniform 70 40000 256 \
    >"$output_dir/simulation_uniform.txt" 2>&1 || true

echo "Running skewed simulated pipeline phase"
"$repo_root/target/release/traffic_profiler" 127.0.0.1:9000 4 "$duration" 8 skewed 70 40000 256 \
    >"$output_dir/simulation_skewed.txt" 2>&1 || true

echo "Matrix artifacts written to $output_dir"
echo "AF_XDP copy/zero-copy rows remain explicitly gated until the runbook is executed on a supported NIC."