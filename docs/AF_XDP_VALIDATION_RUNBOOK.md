# AF_XDP Runtime Validation

This runbook is for a disposable Linux host with a physical NIC and a driver that supports AF_XDP zero-copy. It is not a loopback or WSL test.

The current WSL2 validation built both `libmoe_ebpf_kernel.so` and `xdp_loader`, but a bounded attach probe on WSL `eth0` failed at XSK map creation with `Operation not permitted`. No XDP program was attached. This is a capability result, not a throughput result; complete the runtime gate on native Linux with the required privileges and hardware below.

## Preconditions

- Linux kernel with XDP and AF_XDP support
- Root or equivalent `CAP_NET_ADMIN` and `CAP_BPF` privileges
- A test interface and RX queue that can be interrupted safely
- `clang`, `llvm` including `llc`, `m4`, `libelf-dev`, and `zlib1g-dev`
- A raw Ethernet traffic generator that can target the selected interface and queue
- The pinned nightly Rust toolchain and `bpf-linker`

Do not use a production interface. Record the interface name, driver, kernel version, queue id, MTU, and NIC statistics before starting.

## Build

```bash
cargo +nightly-2025-12-01 install bpf-linker --version 0.9.14
cargo +nightly-2025-12-01 build -Z build-std=core --release -p moe-ebpf-kernel --target bpfel-unknown-none
cargo build --release --bin xdp_loader
```

The workspace includes a `no_std` eBPF crate, so do not run `cargo check --workspace` with the host target. Validate host code with `cargo check -p moe_holistic_engine --all-targets --all-features` and build the kernel with the explicit BPF target command above. The UDP-to-metrics smoke test is available with `bash scripts/telemetry_runtime_smoke.sh`; it does not replace the native-NIC AF_XDP run below.

The experimental `engine::umem_dmabuf` pool maps anonymous host memory or an already-exported DMA-BUF FD. It does not allocate storage from `/dev/vgem`, register memory with `xsk-rs`, or establish AF_XDP zero-copy; those claims require separate exporter, driver, and NIC integration and validation.

The local UDP/synthetic-MoE comparison is available through `bash scripts/telemetry_runtime_smoke.sh` and `bash scripts/run_baremetal_benchmarks.sh` only when configured with a hardware DUT and separate raw-frame generator host. The profiler can embed a Linux `CLOCK_MONOTONIC` nanosecond timestamp in the first eight feature bytes; these bytes are reserved for measurement and are excluded from model features when timing is enabled. `telemetry_demo` reports synthetic completion timing; the AF_XDP loader reports real worker completion timing when `MOE_EMBED_T0=true`. Neither path by itself proves NIC zero-copy; use the native-NIC procedure below and retain all counter snapshots.

For a native AF_XDP run, execute `scripts/run_baremetal_benchmarks.sh` on the DUT after building the BPF object and loader, and copy `target/release/raw_packet_profiler` to a separate generator host. Set `IFACE`, `QUEUE_ID`, `GENERATOR_HOST`, `GENERATOR_IFACE`, `DUT_MAC`, `GENERATOR_SRC_IP`, `DUT_DST_IP`, and optionally `MOE_PEER_RAW_PROFILER`. The script requires `sudo`, `ethtool`, `perf`, and SSH access to the peer; it records interface/driver/NIC snapshots, runs the loader under `perf`, and sends raw Ethernet Token Header v2 frames from the peer. It does not modify offloads, ring sizes, or hugepages. Compare the generator count, NIC deltas, AF_XDP RX count, and processed jobs before claiming a lossless run. Driver-specific zero-copy evidence must still be reviewed independently.
Cross-host latency uses `CLOCK_TAI`, not `CLOCK_MONOTONIC`: monotonic clock epochs are local to each host and cannot be subtracted across machines. Before setting `MOE_PTP_SYNC_CONFIRMED=1`, verify that both hosts are disciplined to the same PTP grandmaster and record the measured clock offset/uncertainty. The harness refuses cross-host timing unless that confirmation is supplied. Report latency uncertainty with the results; if PTP synchronization cannot be established, disable T0 timing for hardware runs and report only same-host durations and throughput.

## Baseline

Run the socket-path baseline first and save its output:

```bash
cargo run --release --bin bench_ingestion -- 127.0.0.1:9000 30
```

Record packets per second, packet loss, CPU utilization, and the exact generator payload configuration.

## AF_XDP run

Set metrics to a protected address and attach to the disposable interface. Ensure the generator's UDP 5-tuple is directed to the selected RX queue with driver-supported RSS/ntuple steering; the loader installs only that queue in XSKMAP and this repository does not configure NIC steering rules.

```bash
export MOE_METRICS_ADDR=127.0.0.1:9100
sudo ./target/release/xdp_loader <interface> <queue-id> \
  target/bpfel-unknown-none/release/libmoe_ebpf_kernel.so
```

Confirm that the loader explicitly reports zero-copy attachment. Generate the same raw Ethernet token workload used for the baseline, then collect:

- `moe_rx_packets_total`, `moe_dispatched_total`, and `moe_processed_jobs_total`
- saturated, invalid, and closed-queue drops
- p95 and p99 `moe_job_latency_seconds`
- NIC RX errors, missed packets, and queue statistics
- process CPU, context switches, and memory bandwidth

The run is valid only when packet accounting agrees across the generator, NIC, AF_XDP RX (`moe_rx_packets_total`), and processed-job metrics within the documented loss budget. The current eBPF program does not expose an independent XDP redirect counter, so do not describe the AF_XDP RX counter as a separate kernel-program counter.

To test the opt-in NACK TX path, set `MOE_NACK_TX_ENABLED=true` and configure `MOE_NACK_SOURCE_MAC`, `MOE_NACK_DESTINATION_MAC`, `MOE_NACK_SOURCE_IP`, `MOE_NACK_DESTINATION_IP`, `MOE_NACK_SOURCE_PORT`, and `MOE_NACK_DESTINATION_PORT` for the test link and cooperating sender. Confirm sequence gaps increment `moe_nack_requests_total`, successful ring submissions increment `moe_nack_tx_sent_total`, and TX exhaustion increments `moe_nack_tx_unavailable_total`. The peer must implement retransmission; the loader only emits requests.

## Rollback and evidence

Stop the loader with `Ctrl+C`; it detaches the XDP program before exit. Restore the interface configuration and compare post-run NIC counters with the baseline. Store the command line, kernel/driver versions, metrics export, Grafana snapshot, and raw benchmark output together so the result is reproducible.

The WSL telemetry demo and UDP traffic profiler are useful for exercising queue behavior and dashboards, but they do not satisfy this zero-copy validation gate.

## DMABUF and NACK boundaries

DMABUF support must be proven by the target kernel, NIC driver, and exporter. A VGEM buffer is a host-side DRM test buffer; it is not GPU VRAM and does not prove GPUDirect or PCIe peer-to-peer DMA. Do not label a VGEM-backed run as GPU-direct.

The userspace sequence tracker can produce bounded `NackRequest` events for missing token sequences. Turning those events into raw Ethernet retransmission requires an AF_XDP TX ring, a defined NACK frame format, sender-side retransmission state, and a loss/ordering contract. The tracker alone does not provide reliable transport semantics.