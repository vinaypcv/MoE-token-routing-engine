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

The parser bounds-checks Ethernet, IPv4 (including variable IHL), UDP, and the token header before reading packet bytes. Matching the protocol marker currently returns `XDP_PASS`; it is a classification scaffold, not a redirect/router, because no XSK or devmap redirect is configured. `XDP_TX` would transmit the same frame back out and is intentionally not used as a routing action.

The user-space loader is built with `cargo build --release --bin xdp_loader`. Attaching is an explicit privileged operation, for example `sudo ./target/release/xdp_loader eth0 [object-path]`; choose an interface that supports the requested XDP mode. XDP attachment can interrupt interface traffic, and WSL's virtual interface/loopback may not support it. The loader is not run by CI.