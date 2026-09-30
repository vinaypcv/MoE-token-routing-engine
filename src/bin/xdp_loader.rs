use aya::maps::XskMap;
use aya::programs::{Xdp, XdpFlags};
use aya::Bpf;
use moe_holistic_engine::engine::backpressure::{
    BackpressureRouter, DEFAULT_EXPERT_COUNT, DEFAULT_EXPERT_QUEUE_CAPACITY,
};
use moe_holistic_engine::engine::telemetry::TelemetryServer;
use std::env;
use std::error::Error;
use std::num::NonZeroU32;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use xsk_rs::config::{
    BindFlags, FrameSize, Interface, LibxdpFlags, QueueSize, SocketConfig, UmemConfig,
};
use xsk_rs::socket::Socket;
use xsk_rs::umem::Umem;

const UMEM_FRAME_COUNT: u32 = 4096;
const RX_RING_SIZE: u32 = 2048;
const RX_BATCH_SIZE: usize = 64;
const XSK_MAP_CAPACITY: u32 = 64;
const FILL_BATCH_SIZE: usize = 64;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    let interface_name = arguments
        .next()
        .ok_or("usage: xdp_loader <interface> [queue-id] [object-path]")?;
    let queue_id = arguments
        .next()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(0);
    if queue_id >= XSK_MAP_CAPACITY {
        return Err(format!("queue id must be less than {XSK_MAP_CAPACITY}").into());
    }
    let object_path = arguments.next().map(PathBuf::from).unwrap_or_else(|| {
        PathBuf::from("target/bpfel-unknown-none/release/libmoe_ebpf_kernel.so")
    });

    let mut bpf = Bpf::load_file(&object_path)?;

    let umem_config = UmemConfig::builder()
        .frame_size(FrameSize::new(4096)?)
        .fill_queue_size(QueueSize::new(RX_RING_SIZE)?)
        .build()?;
    let (umem, mut free_frames) = Umem::new(
        umem_config,
        NonZeroU32::new(UMEM_FRAME_COUNT).expect("frame count is non-zero"),
        false,
    )?;
    let socket_config = SocketConfig::builder()
        .rx_queue_size(QueueSize::new(RX_RING_SIZE)?)
        .libxdp_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD)
        .bind_flags(BindFlags::XDP_ZEROCOPY)
        .build();
    let interface = Interface::from_str(&interface_name)?;

    // SAFETY: this loader creates exactly one AF_XDP socket for the selected interface/queue.
    let (tx_queue, mut rx_queue, queues) =
        unsafe { Socket::new(socket_config, &umem, &interface, queue_id)? };
    let (mut fill_queue, completion_queue) =
        queues.ok_or("AF_XDP socket did not provide fill/completion queues")?;

    // Seed the RX fill ring before publishing the socket in XSK_MAP.
    let seed_count = RX_RING_SIZE as usize;
    let seeded_frames = unsafe {
        fill_queue.produce_and_wakeup(&free_frames[..seed_count], rx_queue.fd_mut(), 100)?
    };
    if seeded_frames != seed_count {
        return Err(format!(
            "only {seeded_frames} of {seed_count} UMEM frames fit in the RX fill ring"
        )
        .into());
    }
    free_frames.drain(..seeded_frames);

    {
        let xsk_map = bpf
            .map_mut("XSK_MAP")
            .ok_or("XSK_MAP not found in eBPF object")?;
        let mut xsk_map = XskMap::try_from(xsk_map)?;
        // SAFETY: rx_queue owns this FD and remains alive through map registration.
        let socket_fd = unsafe { BorrowedFd::borrow_raw(rx_queue.fd().as_raw_fd()) };
        xsk_map.set(queue_id, socket_fd, 0)?;
    }

    let program: &mut Xdp = bpf
        .program_mut("xdp_moe_router")
        .ok_or("XDP program xdp_moe_router not found in object")?
        .try_into()?;
    program.load()?;
    let link_id = program.attach(&interface_name, XdpFlags::default())?;

    println!("Zero-copy AF_XDP redirect attached to {interface_name}, queue {queue_id}.");
    println!("Object: {}", object_path.display());
    println!("Press Ctrl+C to stop receiving and detach.");

    let router = BackpressureRouter::new(
        umem.clone(),
        DEFAULT_EXPERT_COUNT,
        DEFAULT_EXPERT_QUEUE_CAPACITY,
        UMEM_FRAME_COUNT as usize,
    )?;
    let metrics = router.metrics();
    metrics.record_recycled(seeded_frames as u64);

    let metrics_address: std::net::SocketAddr = env::var("MOE_METRICS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9100".to_owned())
        .parse()?;
    let telemetry = Arc::clone(&metrics);
    let telemetry_task = tokio::spawn(async move {
        if let Err(error) = TelemetryServer::run(metrics_address, telemetry).await {
            eprintln!("Prometheus metrics server stopped: {error}");
        }
    });

    let running = Arc::new(AtomicBool::new(true));
    let receiver_running = Arc::clone(&running);
    let receiver_umem = umem.clone();
    let receiver_metrics = Arc::clone(&metrics);
    let receiver = thread::spawn(move || -> std::io::Result<()> {
        let mut descriptors = vec![xsk_rs::FrameDesc::default(); RX_BATCH_SIZE];
        let mut fill_batch = [xsk_rs::FrameDesc::default(); FILL_BATCH_SIZE];

        while receiver_running.load(Ordering::Acquire) {
            let recycle_capacity = UMEM_FRAME_COUNT as usize - free_frames.len();
            router.drain_recycled(&mut free_frames, recycle_capacity);

            while !free_frames.is_empty() {
                let batch_len = free_frames.len().min(FILL_BATCH_SIZE);
                for slot in fill_batch.iter_mut().take(batch_len) {
                    *slot = free_frames
                        .pop()
                        .expect("batch length is bounded by free frames");
                }
                let submitted = unsafe {
                    fill_queue.produce_and_wakeup(
                        &fill_batch[..batch_len],
                        rx_queue.fd_mut(),
                        100,
                    )?
                };
                receiver_metrics.record_recycled(submitted as u64);
                for descriptor in fill_batch.iter().take(batch_len).skip(submitted) {
                    free_frames.push(*descriptor);
                }
                if submitted == 0 {
                    break;
                }
            }

            let received = unsafe { rx_queue.poll_and_consume(&mut descriptors, 25)? };
            for descriptor in descriptors.iter().take(received).copied() {
                let packet = unsafe { receiver_umem.data(&descriptor) };
                match router.dispatch_frame(descriptor, packet.contents()) {
                    Ok(()) => {}
                    Err((rejected_frame, _reason)) => free_frames.push(rejected_frame),
                }
            }
        }

        // The program is detached before shutdown; recycle anything already queued in RX.
        loop {
            let received = unsafe { rx_queue.poll_and_consume(&mut descriptors, 0)? };
            if received == 0 {
                break;
            }
            free_frames.extend(descriptors.iter().take(received).copied());
        }

        free_frames.extend(router.shutdown());
        drop((fill_queue, rx_queue));
        Ok(())
    });

    tokio::signal::ctrl_c().await?;
    running.store(false, Ordering::Release);
    program.detach(link_id)?;
    telemetry_task.abort();
    receiver
        .join()
        .map_err(|_| std::io::Error::other("AF_XDP receive thread panicked"))??;
    drop(bpf);
    drop((tx_queue, completion_queue, umem));
    println!("Detached XDP program.");
    println!(
        "Received: {}",
        metrics.rx_packets_total.load(Ordering::Relaxed)
    );
    println!(
        "Dispatched: {}",
        metrics.dispatched_total.load(Ordering::Relaxed)
    );
    println!(
        "Processed: {}",
        metrics.processed_jobs.load(Ordering::Relaxed)
    );
    println!(
        "Saturated drops: {}",
        metrics.saturated_drops_total.load(Ordering::Relaxed)
    );
    println!(
        "Invalid packets: {}",
        metrics.invalid_packets_total.load(Ordering::Relaxed)
    );
    Ok(())
}
