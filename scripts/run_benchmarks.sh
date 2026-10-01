#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[1/4] Checking formatting"
cargo fmt --all -- --check

echo "[2/4] Running clippy"
cargo clippy -p moe_holistic_engine --all-targets --all-features -- -D warnings

echo "[3/4] Running host tests"
cargo test -p moe_holistic_engine --no-fail-fast

echo "[4/4] Running quantization benchmark"
cargo bench --bench quantization_profiles

echo "Benchmark verification complete."