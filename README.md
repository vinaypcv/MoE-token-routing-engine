# moe-token-routing-engine

A Rust prototype for token-routing lifecycle experiments. It combines a persistent, core-pinned worker pool, Linux UDP batching (`sendmmsg`/`recvmmsg`), atomic lifecycle tracking, and CPU-backed mock GPU memory.

The GPU memory and network pipeline are simulations: this project does not currently map physical GPU memory or provide true zero-copy networking.

## Requirements

- Stable Rust toolchain with Cargo
- Linux for batched `sendmmsg`/`recvmmsg`; other platforms use the socket fallback
- On Windows, WSL2 Ubuntu is the supported environment for the Linux-specific path

## Build and run

```bash
cargo check
cargo run --release
```

## Benchmark

Run the standalone release benchmark, which performs five warmup sweeps and measures twenty 10,000-token sweeps:

```bash
cargo bench --bench holistic_benchmarks
```

The benchmark reports measured-phase throughput and total datagrams sent and received, including warmup sweeps.

For a Linux `recvmmsg` ingestion baseline with a loopback packet generator:

```bash
cargo run --release --bin bench_ingestion -- 127.0.0.1:9000 3
```

For a repeatable uniform-versus-skewed traffic report on Linux, run `bash scripts/run_traffic_benchmark_report.sh 127.0.0.1:9000 4 30 8 40000 256 benchmark-report.csv` while the telemetry demo is listening. The script writes one CSV row per traffic phase plus a companion JSON file containing the Git revision, kernel, timestamp, and run parameters.

For the comparison matrix, run `bash scripts/run_baseline_matrix.sh 30 benchmark-matrix`; it records the standard UDP and simulated phases plus explicit native-NIC gates for AF_XDP copy and zero-copy.

For the FP32 versus INT8 quantization profile, run `cargo bench --bench quantization_profiles`. It reports throughput and p99 latency for the current owned-buffer quantizer; it is not a claim about in-place VRAM quantization.
For one-command reviewer verification, run `bash scripts/run_benchmarks.sh`; it executes formatting, clippy, host tests, and the quantization benchmark in sequence.

The `NackTxQueue` adapter uses the real `xsk-rs` TX and completion-ring APIs to submit fixed-size NACK frames from UMEM-owned descriptors. Live loader wiring still requires reserving TX frames and adding a sequence field to the token packet contract; the current 10-byte header has no independent sequence identifier.

To listen for an external UDP generator instead, append `--external`. This measures the normal UDP socket path, not AF_XDP; see [docs/ARCHITECTURE_EBPF_AF_XDP.md](docs/ARCHITECTURE_EBPF_AF_XDP.md) for the AF_XDP measurement caveats.

Generate batched UDP token traffic with a token header followed by deterministic feature bytes. Arguments configure workers, distribution, duration, rate limit, and feature length:

```bash
cargo run --release --bin traffic_profiler -- 127.0.0.1:9000 4 5 8 skewed 70 0 256
```

This sends UDP application payloads for socket-path testing; the operating system supplies the Ethernet/IP/UDP headers. It does not generate raw Ethernet frames or guarantee traffic reaches an AF_XDP-capable NIC queue.

The profiler arguments are `<target-addr> <threads> [seconds] [experts] [uniform|skewed] [hot-percent] [total-pps] [feature-bytes]`. Use `total-pps` of `0` for unpaced sending; feature bytes default to 256.

When `xdp_loader` is attached, it starts a Prometheus text endpoint at `http://127.0.0.1:9100/metrics`; set `MOE_METRICS_ADDR` to override the bind address. The default expert worker currently runs a small synchronous linear-model placeholder over feature bytes following the 10-byte token header. Packets containing only the header are reported as execution errors; replace the model implementation when defining the production tensor payload format.

To preview the live dashboard without an XDP-capable NIC, run `cargo run --release --bin telemetry_demo` and open `http://127.0.0.1:9100/`. This mode counts UDP packets sent to `MOE_DEMO_UDP_ADDR` (default `127.0.0.1:9000`) and simulates bounded per-expert worker service; its counters are not AF_XDP or NIC telemetry. Set `MOE_METRICS_ADDR` to change the dashboard/metrics bind address, `MOE_DEMO_QUEUE_CAPACITY` to adjust simulated queue bounds, and `MOE_DEMO_SERVICE_PER_TICK` to adjust simulated service capacity.

Set `MOE_DEMO_ADAPTIVE_BACKPRESSURE=true` to let the synthetic service capacity increase near aggregate queue saturation and return toward its configured baseline as queues drain. The current synthetic capacity is exported as `moe_service_capacity_per_tick`.
Set `MOE_DEMO_LOAD_SHEDDING` to `oldest` (FIFO default), `priority` (reserve capacity for Expert 0), or `expert-aware` (tighten repeatedly dropping expert queues). Each rejection remains visible through `moe_drop_reason_total{reason="queue_full"}`.

For the real AF_XDP loader, set `MOE_ROUTING_TOP_K` above `1` to enable token-aware top-k prediction and `MOE_ROUTING_CONFIDENCE` to reject low-confidence predictions. Prediction, reroute, and low-confidence counters are exported as `moe_predictions_total`, `moe_reroutes_total`, and `moe_low_confidence_predictions_total`.
Set `MOE_FALLBACK_EXPERT_ID` to a reserved expert slot, such as `7`, to route saturated primary queues to that fallback slot at the 90% watermark. This is safe for the current raw-byte token format; INT8 conversion requires an explicit FP32 payload contract and is exposed as a standalone `ElasticQuantizer` policy.

Import [docs/grafana/moe-af-xdp-dashboard.json](docs/grafana/moe-af-xdp-dashboard.json) into Grafana and select the Prometheus source scraping `/metrics`. The endpoint binds to loopback by default; to scrape from a separate host or container, set `MOE_METRICS_ADDR=0.0.0.0:9100` and restrict network access appropriately.

Open the public dashboard at http://localhost:3000/public-dashboards/151c235a7d814048aae07aa1510b896f.
Additional public dashboard view: http://localhost:3000/public-dashboards/b459c24ff44a49d48730d311b710d908?from=now-5m&to=now&timezone=browser.
Recorded simulation snapshot: http://localhost:3000/dashboard/snapshot/3J7xlcnw6d0JY3foK4lojqL4MfiTQfGf (17:42:40-17:47:40 local time; includes latency SLO, phase latency, drop reasons, and routing panels).

For the complete local Phase 2 preview (Prometheus, Grafana, UDP receiver, and uniform/skewed traffic phases), start Docker and run:

```bash
sudo apt-get install -y clang llvm m4 libelf-dev zlib1g-dev
bash dashboards/run_traffic_storm_demo.sh
```

The script builds the required Rust binaries, starts UDP-driven simulated worker telemetry plus Prometheus/Grafana, and runs uniform and hot-expert traffic phases. It prints the URLs and cleanup commands. For Docker Desktop to scrape the host, the script binds metrics to `0.0.0.0:9100`; keep that port firewalled to trusted local/demo traffic. Queue service is simulated; this preview does not attach XDP or measure NIC/AF_XDP zero-copy performance.

See [docs/ARCHITECTURE_EBPF_AF_XDP.md](docs/ARCHITECTURE_EBPF_AF_XDP.md) for the XDP parser, verifier, UMEM, and ring ownership specification.
See [docs/AF_XDP_VALIDATION_RUNBOOK.md](docs/AF_XDP_VALIDATION_RUNBOOK.md) for the real-NIC zero-copy validation procedure and acceptance criteria.

## Speculative routing prototype

Run the experimental prediction/validation state-machine simulation with:

```bash
cargo run --release --bin speculative_kernel
```

The prototype sends loopback UDP packets using synthetic predicted and actual expert IDs, then records matches and invalidations with atomic state transitions. It does not consume intermediate attention states, implement a learned top-k predictor, route to physical GPUs, or guarantee zero network latency. The elapsed time is the measured simulation runtime, not a measurement of latency hidden behind real model computation.

## eBPF/XDP classifier prototype

Build the separate no-std XDP object with nightly Rust, `rust-src`, and `bpf-linker`:

```bash
cargo +nightly-2025-12-01 install bpf-linker --version 0.9.14
cargo +nightly-2025-12-01 build -Z build-std=core --release -p moe-ebpf-kernel --target bpfel-unknown-none
```

The parser bounds-checks Ethernet, IPv4 (including variable IHL), UDP, and the token header before reading packet bytes. Matching the protocol marker redirects to the XSKMAP entry for the packet's RX queue; missing entries and non-matching traffic fall back to `XDP_PASS`.

The user-space loader is built with `cargo build --release --bin xdp_loader`. Attaching is an explicit privileged operation, for example `sudo ./target/release/xdp_loader eth0 0 [object-path]`; choose an interface and RX queue that support AF_XDP. XDP attachment can interrupt interface traffic, and WSL's virtual interface/loopback may not support zero-copy mode. The loader is not run by CI.

The AF_XDP loader requires `clang`, `llvm` (including `llc`), `m4`, `libelf-dev`, and `zlib1g-dev` at build time. It allocates UMEM, seeds the fill ring, requests `XDP_ZEROCOPY` explicitly, inserts the socket FD into XSKMAP at the selected queue index, and recycles received descriptors. Socket creation fails rather than silently falling back to copy mode when the selected interface cannot provide zero-copy support. The current receive loop intentionally recycles frames without application-level processing.