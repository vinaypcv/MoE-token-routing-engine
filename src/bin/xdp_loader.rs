use aya::maps::XskMap;
use aya::programs::{Xdp, XdpFlags};
use aya::Bpf;
use std::env;
use std::error::Error;
use std::num::NonZeroU32;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use xsk_rs::config::{BindFlags, Interface, LibxdpFlags, SocketConfig, UmemConfig};
use xsk_rs::socket::Socket;
use xsk_rs::umem::Umem;

const UMEM_FRAME_COUNT: u32 = 2048;
const RX_BATCH_SIZE: usize = 64;
const XSK_MAP_CAPACITY: u32 = 64;

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

    let (umem, mut free_frames) = Umem::new(
        UmemConfig::default(),
        NonZeroU32::new(UMEM_FRAME_COUNT).expect("frame count is non-zero"),
        false,
    )?;
    let socket_config = SocketConfig::builder()
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
    let seeded_frames =
        unsafe { fill_queue.produce_and_wakeup(&free_frames, rx_queue.fd_mut(), 100)? };
    if seeded_frames != free_frames.len() {
        return Err(format!(
            "only {seeded_frames} of {} UMEM frames fit in the RX fill ring",
            free_frames.len()
        )
        .into());
    }
    free_frames.clear();

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

    let running = Arc::new(AtomicBool::new(true));
    let receiver_running = Arc::clone(&running);
    let receiver_umem = umem.clone();
    let receiver = thread::spawn(move || -> std::io::Result<u64> {
        let mut descriptors = vec![xsk_rs::FrameDesc::default(); RX_BATCH_SIZE];
        let mut packet_count = 0u64;

        while receiver_running.load(Ordering::Acquire) {
            let received = unsafe { rx_queue.poll_and_consume(&mut descriptors, 100)? };
            if received == 0 {
                continue;
            }

            packet_count += received as u64;
            let recycled = unsafe {
                fill_queue.produce_and_wakeup(&descriptors[..received], rx_queue.fd_mut(), 100)?
            };
            if recycled != received {
                return Err(std::io::Error::other(format!(
                    "recycled {recycled} of {received} RX frames"
                )));
            }
        }

        drop(receiver_umem);
        Ok(packet_count)
    });

    tokio::signal::ctrl_c().await?;
    running.store(false, Ordering::Release);
    let received_packets = receiver
        .join()
        .map_err(|_| std::io::Error::other("AF_XDP receive thread panicked"))??;

    program.detach(link_id)?;
    drop(bpf);
    drop((tx_queue, completion_queue, umem));
    println!("Detached XDP program; received {received_packets} redirected packets.");
    Ok(())
}
