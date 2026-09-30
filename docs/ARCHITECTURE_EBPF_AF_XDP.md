# eBPF and AF_XDP Architecture Specification

## 1. Scope and status

The XDP component classifies Ethernet/IPv4/UDP frames and redirects matching token packets to an AF_XDP socket selected by receive queue. The kernel object is `moe-ebpf-kernel`; the user-space attachment and UMEM consumer are `xdp_loader`.

The implementation requests AF_XDP zero-copy mode. Socket creation fails if the selected interface/queue cannot provide that mode. A successful build does not prove successful attachment, verifier acceptance on a particular kernel, driver support, or zero-copy operation on the host. Those require a privileged runtime test on a supported Linux interface and driver.

The loader parses token headers in place and dispatches descriptor ownership through bounded per-expert channels. Worker threads currently validate the marker and return frames; they do not perform model inference, transmit responses, or measure AF_XDP throughput.

## 2. Packet path

```text
NIC RX queue
    |
    v
XDP hook: xdp_moe_router
    |  parse Ethernet -> IPv4 -> UDP -> token header
    |  non-match, malformed, fragmented, or unavailable XSK: XDP_PASS
    |  matching magic byte and populated queue entry: XDP_REDIRECT
    v
XSKMAP[RX queue id]
    |
    v
AF_XDP socket RX ring <---- UMEM frame addresses from fill ring
    |
    v
xdp_loader parses in UMEM and dispatches the descriptor to a bounded expert queue
    |
    v
worker completes placeholder work and returns descriptor through recycler
    |
    v
ingress loop returns descriptor to the fill ring
```

AF_XDP zero-copy mode can allow a supported NIC driver to DMA packet data into UMEM-backed frames. It is not an unconditional property of XDP or AF_XDP: it depends on the driver, device, kernel, bind flags, and interface queue. `XDP_ZEROCOPY` requests that mode and should fail rather than silently fall back when unsupported.

## 3. UMEM and ring ownership

The loader creates one UMEM and one AF_XDP socket for the selected `(interface, queue_id)` pair. The socket FD is installed at the same queue index in `XSK_MAP`; XSKMAP redirect succeeds only when the target socket is bound to the RX queue on which the packet arrived.

- **UMEM**: page-aligned memory managed by the AF_XDP library and divided into fixed-size frames. The current loader configures 4,096 frames of 4,096 bytes each (16 MiB total backing memory).
- **Fill ring**: initially populated with free UMEM frame descriptors. The kernel uses these frames for incoming packets.
- **RX ring**: carries descriptors identifying received UMEM frame offsets and lengths. Packet bytes remain in UMEM; the descriptor is metadata, not a copy of the packet.
- **Completion ring**: is created with the socket for transmit completion accounting. The current loader retains it but does not transmit packets.
- **Frame recycling**: after consumption, descriptors are returned to the fill ring. A frame must not be read or reused by the application while the kernel owns it.

The loader seeds the fill ring before publishing the socket in XSKMAP and attaching the XDP program. It keeps a 4,096-frame UMEM pool, a 2,048-entry fill/RX ring, and bounded per-expert channels (1,024 jobs each by default). Invalid frames and full/closed expert queues are counted and returned to the ingress-owned free-frame pool immediately. Completed jobs return descriptors over a bounded recycler channel sized to the UMEM pool. Separate expert queues isolate a busy expert's backlog from other expert workers; the current single ingress poller remains a shared throughput limit. It currently manages one queue per process invocation. Multi-queue deployments need one correctly bound socket and populated map entry per active queue (or another explicit queue-selection design).

## 4. Packet layout and parser rules

At the XDP hook, the frame begins with Ethernet. For an Ethernet II IPv4 packet without IP options, the UDP header begins at byte 34 and UDP payload at byte 42. The payload offset is not universally 42: IPv4's IHL field expresses the header size in 32-bit words, so the UDP offset is `EthHdr::LEN + IHL * 4`, and the token starts after the UDP header.

The current parser performs checked offset/length calculations and validates packet data against `data_end` before byte access. It also:

1. Checks the Ethernet frame range and reads EtherType as bytes, avoiding an unchecked enum load from packet memory.
2. Checks the minimum IPv4 header, validates version and IHL, and bounds-checks the complete variable-length IPv4 header.
3. Accepts UDP only and passes fragmented IPv4 packets, since a fragment may not contain the full transport header/payload.
4. Validates IPv4 total length and UDP length before reading the token header.
5. Checks the complete fixed-size token header range and compares its first byte to `0x77`.
6. Redirects only when the RX queue index is within XSKMAP capacity; absent/unusable XSKMAP entries fall back to `XDP_PASS`.

The token structure is packed. The parser reads the magic byte from a checked byte offset and does not form references to potentially unaligned packed multi-byte fields. VLAN-tagged Ethernet is not currently parsed; such frames do not match this IPv4-at-fixed-Ethernet-offset parser and pass.

## 5. Verifier safety invariants

The BPF verifier reasons about packet pointers as ranges bounded by `data` and `data_end`. Every dereference must be dominated by a successful range check. Offset arithmetic must not wrap; this implementation uses checked additions for ranges and computed header offsets.

Other relevant constraints:

- Keep packet reads small, fixed-width, and dominated by explicit bounds checks.
- Do not trust lengths from packet fields until validating their minimum and relationship to enclosing lengths.
- Do not treat IPv4 fragments as complete UDP datagrams.
- Keep loops bounded and avoid unbounded pointer arithmetic; the current parser has no packet-sized loop.
- A valid local BPF ELF build is not verifier acceptance. The kernel verifier runs at program load/attach and is specific to kernel version, configuration, and program characteristics.

## 6. XDP actions and fallback behavior

For matching packets with a populated XSKMAP queue entry, `XskMap::redirect(queue_id, 0)` requests XDP redirection to AF_XDP. If the map entry is missing or redirect lookup fails, the implementation returns `XDP_PASS`. Non-IPv4, non-UDP, malformed, fragmented, truncated, and non-matching packets also pass through the regular networking stack.

`XDP_TX` is not used: it transmits the same received frame back through the ingress device, not to another expert, queue, or GPU. `XDP_REDIRECT` to XSKMAP is the AF_XDP handoff mechanism; further application-level routing is not implemented yet.

## 7. Build and run

The BPF artifact uses the pinned nightly and linker setup documented in the repository README and CI workflow. The user-space loader can be built with:

```bash
cargo build --release --bin xdp_loader
```

Attach manually on a supported Linux host with the required BPF/network capabilities and a compatible interface/queue:

```bash
sudo ./target/release/xdp_loader <interface> <queue-id> [bpf-object-path]
```

The default object path is `target/bpfel-unknown-none/release/libmoe_ebpf_kernel.so`. Attachment can change packet handling on the selected interface. Validate on a disposable/test interface before production use. WSL virtual interfaces and loopback commonly do not provide AF_XDP zero-copy support.

## 8. Performance interpretation

| Path | Data path | What batching/zero-copy means | Caveats |
|---|---|---|---|
| `UdpSocket::recv_from` | NIC -> kernel networking stack -> socket buffer -> application | One receive call per datagram in the basic API | Includes normal networking stack work and payload copy to user space |
| `recvmmsg` | Same socket-buffer path | One syscall can return multiple datagrams | Still uses the normal stack and socket buffers; not kernel bypass |
| AF_XDP copy mode | XDP -> AF_XDP socket/UMEM | Avoids ordinary socket delivery after XDP redirect | Kernel may copy packet data into UMEM |
| AF_XDP zero-copy mode | Supported driver/NIC -> UMEM frame -> RX descriptor | Avoids the packet-data copy into UMEM when the driver supports it | Still has syscalls/control work, ring synchronization, and driver/NAPI processing; may require polling |

Do not interpret `recvmmsg` as eliminating context switches: it amortizes receive syscall overhead across a batch. AF_XDP zero-copy does not mean zero syscalls, zero CPU work, or guaranteed zero latency. Throughput depends on packet size, CPU/NUMA placement, NIC and driver, queue count, interrupt/polling mode, kernel configuration, and generator capacity. No fixed packets-per-second figures are claimed here; use repeatable measurements on the target hardware.

The repository's earlier loopback UDP figures are simulation measurements, not NIC throughput or an AF_XDP comparison. `bench_ingestion` provides a batched UDP receive baseline, while `traffic_profiler` generates batched UDP token-header traffic with uniform or hot-expert distributions. Neither is a raw-Ethernet AF_XDP load generator. AF_XDP measurement still requires a supported interface, an appropriate traffic generator, and runtime instrumentation of the AF_XDP consumer.
