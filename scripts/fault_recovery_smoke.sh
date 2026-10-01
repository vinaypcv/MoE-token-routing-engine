#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
metrics_url="${MOE_METRICS_URL:-http://127.0.0.1:9100/metrics}"
udp_host="${MOE_DEMO_UDP_HOST:-127.0.0.1}"
udp_port="${MOE_DEMO_UDP_PORT:-9000}"
log_file="${TMPDIR:-/tmp}/moe-fault-recovery.log"

if [[ ! -x "$repo_root/target/release/telemetry_demo" ]]; then
    echo "Build telemetry_demo before running this smoke test." >&2
    exit 1
fi

cleanup() {
    if [[ -n "${demo_pid:-}" ]]; then
        kill "$demo_pid" 2>/dev/null || true
        wait "$demo_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

start_demo() {
    MOE_METRICS_ADDR=127.0.0.1:9100 MOE_DEMO_UDP_ADDR="$udp_host:$udp_port" \
        "$repo_root/target/release/telemetry_demo" >"$log_file" 2>&1 &
    demo_pid=$!
    for _ in $(seq 1 50); do
        curl --silent --fail "$metrics_url" >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    echo "telemetry demo did not become ready" >&2
    return 1
}

start_demo
python3 - "$udp_host" "$udp_port" <<'PY'
import socket
import sys

target = (sys.argv[1], int(sys.argv[2]))
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
for payload in (b"", b"bad", bytes([0x77]) + b"\x00" * 9):
    sock.sendto(payload, target)
sock.close()
PY

invalid_before="$(curl --silent "$metrics_url" | awk '$1 == "moe_invalid_packets_total" {print $2}')"
[[ "${invalid_before:-0}" -ge 1 ]] || {
    echo "fault injection did not increment invalid packet telemetry" >&2
    exit 1
}

kill "$demo_pid" 2>/dev/null || true
wait "$demo_pid" 2>/dev/null || true
demo_pid=''
start_demo
curl --silent --fail "$metrics_url" >/dev/null
echo "fault injection and telemetry restart recovery passed"