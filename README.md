# MoE Token Routing Engine

[![CI](https://github.com/vinaypcv/MoE-token-routing-engine/actions/workflows/verify-engine.yml/badge.svg)](https://github.com/vinaypcv/MoE-token-routing-engine/actions/workflows/verify-engine.yml)

A Rust and eBPF prototype for routing Mixture-of-Experts tokens through a bounded, observable data plane.

The project explores the boundary between packet ingestion and inference backpressure:

```text
Ethernet / IPv4 / UDP
        |
        v
XDP parser -> XSKMAP -> AF_XDP UMEM -> dispatcher
                                      |
                         +------------+------------+
                         v                         v
                 primary expert queue       fallback / shedding
                                      |
                                      v
                              worker execution
```

The repository supports two deliberately separate modes:

- **Synthetic mode**: loopback UDP, simulated worker service, adaptive backpressure, Grafana telemetry, and reproducible local benchmarks.
- **AF_XDP mode**: a native Linux loader that requests zero-copy AF_XDP on a selected NIC queue and dispatches UMEM frames to bounded expert workers.

Synthetic results are not presented as NIC or GPU performance measurements. GPU DMABUF, GPUDirect, NIC-to-VRAM DMA, and physical zero-copy throughput require hardware validation outside WSL.

## Verified Results

The current repository validates:

- Rust host engine with 15 unit tests and zero Clippy warnings under `-D warnings`.
- eBPF object build for `bpfel-unknown-none` with pinned nightly Rust and `bpf-linker`.
- AF_XDP userspace loader build.
- Token Header v2 parsing with a 14-byte header:
  - `magic_byte: u8`
  - `token_id: u64` big-endian
  - `expert_id: u8`
  - `sequence_id: u32` big-endian
- Bounded expert queues with explicit drop reasons and fallback routing policy.
- Runtime-detected AVX2 quantization with scalar fallback.
- Allocation-free NACK frame formatting and a tested `xsk-rs` TX/completion-ring adapter. The adapter is not active retransmission in the live receiver loop.
- Prometheus/Grafana dashboards, SLO alerts, benchmark metadata, and fault-recovery smoke tests.

Representative local quantization output varies by host. Recent runs measured roughly 2.7M FP32 samples/s and 2.4M AVX2 INT8 samples/s with sub-microsecond p99 latency. The INT8 path trades CPU work for a smaller representation; it is not claimed to be faster than the FP32 baseline.

## Quick Start

### Requirements

- Stable Rust and Cargo.
- Linux for `sendmmsg`/`recvmmsg` and AF_XDP paths.
- WSL2 Ubuntu is suitable for the synthetic Linux path, but not for certifying physical-NIC zero-copy.
- Docker Desktop for the Prometheus/Grafana stack.

Build the host package:

```bash
cargo check -p moe_holistic_engine
cargo build --release --bin telemetry_demo --bin traffic_profiler
```

Run the full local quality gate:

```bash
bash scripts/run_benchmarks.sh
```

This runs formatting, Clippy, host tests, and the quantization benchmark.

## Live Synthetic Demonstration

Start telemetry on isolated ports so it does not collide with another demo:

```bash
MOE_METRICS_ADDR=127.0.0.1:9112 \
MOE_DEMO_UDP_ADDR=127.0.0.1:9012 \
MOE_DEMO_ADAPTIVE_BACKPRESSURE=true \
MOE_DEMO_LOAD_SHEDDING=expert-aware \
./target/release/telemetry_demo
```

In another terminal, generate uniform and hot-expert traffic:

```bash
./target/release/traffic_profiler 127.0.0.1:9012 4 10 8 uniform 70 40000 256
./target/release/traffic_profiler 127.0.0.1:9012 4 10 8 skewed 80 40000 256
```

Inspect the live metrics:

```bash
curl -s http://127.0.0.1:9112/metrics \
  | grep -E 'moe_(rx_packets_total|dispatched_total|processed_jobs_total|drop_reason_total|service_capacity_per_tick)'
```

This path demonstrates queue protection, deterministic load shedding, adaptive service capacity, latency histograms, and Prometheus export. It does not execute XDP or physical NIC DMA. In the synthetic demo, `fallback_routed_total` and `nack_requests_total` remain zero because those counters belong to the AF_XDP dispatcher path.

## Grafana

Start Prometheus and Grafana:

```bash
docker compose -f monitoring/compose.yml up -d --wait
```

Grafana credentials:

```text
Username: admin
Password: moe-demo-local
```

Live authenticated dashboard:

<http://localhost:3000/d/afxdp-moe-telemetry/af-xdp-mixture-of-experts-kernel-bypass-telemetry?orgId=1&from=now-5m&to=now&refresh=5s>

Public dashboard:

<http://localhost:3000/public-dashboards/b459c24ff44a49d48730d311b710d908?from=now-5m&to=now&timezone=browser>

The public view is intentionally compact. The authenticated dashboard includes phase latency, p50/p95/p99/p99.9 SLO views, drop reasons, fallback routing, and NACK metrics.

A recorded simulation snapshot is available at:

<http://localhost:3000/dashboard/snapshot/jgPLCcfVyjnaDbJaAoXz0rlVs2IO1Kgb>

Import the dashboard JSON directly with [docs/grafana/moe-af-xdp-dashboard.json](docs/grafana/moe-af-xdp-dashboard.json) when using another Grafana instance.

## Benchmarks and Evidence

Run the standard loopback ingestion baseline:

```bash
cargo run --release --bin bench_ingestion -- 127.0.0.1:9000 10
```

Generate a repeatable uniform-versus-skewed CSV report with JSON provenance:

```bash
bash scripts/run_traffic_benchmark_report.sh \
  127.0.0.1:9000 4 30 8 40000 256 benchmark-report.csv
```

Run the comparison matrix:

```bash
bash scripts/run_baseline_matrix.sh 30 benchmark-matrix
```

The matrix records standard UDP and simulation phases. AF_XDP copy and zero-copy rows remain explicit native-NIC gates rather than simulated claims.

GitHub Actions runs host validation, the simulation evidence report, the eBPF build, and the AF_XDP loader build. Successful runs upload binaries, checksums, commit metadata, CSV/JSON benchmark evidence, and telemetry logs.

## AF_XDP Build and Run

Install the pinned eBPF toolchain on native Linux or WSL:

```bash
cargo +nightly-2025-12-01 install bpf-linker --version 0.9.14
cargo +nightly-2025-12-01 build \
  -Z build-std=core \
  --release \
  -p moe-ebpf-kernel \
  --target bpfel-unknown-none
cargo build --release --bin xdp_loader
```

Attach on a disposable native Linux interface and RX queue:

```bash
sudo ./target/release/xdp_loader \
  <interface> <queue-id> \
  target/bpfel-unknown-none/release/libmoe_ebpf_kernel.so
```

Optional routing configuration:

```bash
export MOE_ROUTING_TOP_K=2
export MOE_ROUTING_CONFIDENCE=0.7
export MOE_FALLBACK_EXPERT_ID=7
export MOE_METRICS_ADDR=127.0.0.1:9100
```

The loader explicitly requests `XDP_ZEROCOPY` and fails rather than silently falling back. WSL loopback and virtual interfaces do not satisfy the zero-copy validation gate.

Use [docs/AF_XDP_VALIDATION_RUNBOOK.md](docs/AF_XDP_VALIDATION_RUNBOOK.md) for kernel, driver, NIC counter, packet-accounting, rollback, and evidence requirements.

## Architecture Notes

- `moe-ebpf-kernel/src/main.rs`: verifier-safe Ethernet, IPv4, UDP, Token Header v2 parsing and XSKMAP redirect.
- `src/bin/xdp_loader.rs`: UMEM, AF_XDP socket, XSKMAP registration, zero-copy request, and frame lifecycle.
- `src/engine/dispatcher.rs`: parsing, prediction, fallback routing, queue admission, drop accounting, and sequence-gap observation.
- `src/engine/backpressure.rs`: bounded expert queues and worker lifecycle.
- `src/engine/elastic_quant.rs`: owned-buffer INT8 quantization and watermark policy.
- `src/engine/nack.rs`: sequence tracking and fixed 52-byte NACK frame formatting.
- `src/engine/nack_tx.rs`: `xsk-rs` TX/completion-ring adapter with explicit UMEM ownership.
- `src/engine/telemetry.rs`: Prometheus counters, histograms, phase latency, and expert metrics.

## Scope Boundary

This project is a research and systems prototype. It does not currently claim:

- GPU VRAM-backed AF_XDP UMEM.
- GPUDirect or NIC-to-VRAM peer-to-peer DMA.
- Production MoE model execution.
- Active NACK retransmission in the live loader loop.
- Physical-NIC AF_XDP throughput, loss, or latency results from WSL.

Those claims require a supported native Linux NIC, driver, privileges, traffic generator, and reproducible hardware evidence. See the validation runbook before making them.

## Related Documentation

- [eBPF and AF_XDP architecture](docs/ARCHITECTURE_EBPF_AF_XDP.md)
- [AF_XDP validation runbook](docs/AF_XDP_VALIDATION_RUNBOOK.md)
- [Grafana dashboard JSON](docs/grafana/moe-af-xdp-dashboard.json)
- [Prometheus alerts](dashboards/prometheus_rules.yml)
