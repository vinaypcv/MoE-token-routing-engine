#![allow(dead_code)]

pub mod engine;

use std::io::ErrorKind;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const STATE_INIT: u8 = 0;
const STATE_PAGED: u8 = 1;
const STATE_IN_FLIGHT: u8 = 2;
const STATE_COMPLETE: u8 = 3;

const NUM_GPUS: usize = 4;
const GPU_PAGE_SIZE: usize = 4096;
const EXPERT_SLOTS_PER_GPU: usize = 4;
const WORKER_COUNT: usize = 4;
const RECEIVE_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const RECEIVE_BATCH_SIZE: usize = 32;

#[repr(C, align(64))]
pub struct HolisticTokenContext {
    pub token_id: u64,
    pub tensor_dimension: u32,
    pub source_gpu_id: u8,
    pub destination_gpu_id: u8,
    pub target_expert_slot: u8,
    pub payload_bytes: [u8; 64],
    pub lifecycle_state: AtomicU8,
}

impl HolisticTokenContext {
    pub fn load_lifecycle_state(&self) -> u8 {
        self.lifecycle_state.load(Ordering::Acquire)
    }
}

pub struct VirtualGpuDevice {
    pub gpu_id: usize,
    mapped_memory: Vec<AtomicU8>,
    pub mapped_capacity: usize,
}

impl VirtualGpuDevice {
    fn new(gpu_id: usize) -> Self {
        let mapped_capacity = GPU_PAGE_SIZE * EXPERT_SLOTS_PER_GPU;
        let mapped_memory = (0..mapped_capacity).map(|_| AtomicU8::new(0)).collect();

        Self {
            gpu_id,
            mapped_memory,
            mapped_capacity,
        }
    }

    fn write_expert_byte(&self, expert_slot: usize, value: u8) {
        let offset = expert_slot * GPU_PAGE_SIZE;
        if let Some(byte) = self.mapped_memory.get(offset) {
            byte.store(value, Ordering::Relaxed);
        }
    }
}

pub struct HolisticRoutingKernel {
    pub cluster_nodes: usize,
    pub active_pool: Vec<HolisticTokenContext>,
    pub virtual_gpus: Vec<VirtualGpuDevice>,
}

impl HolisticRoutingKernel {
    pub fn new(cluster_nodes: usize, pool_capacity: usize) -> Self {
        let active_pool = (0..pool_capacity)
            .map(|index| HolisticTokenContext {
                token_id: index as u64,
                tensor_dimension: 4096,
                source_gpu_id: (index % NUM_GPUS) as u8,
                destination_gpu_id: ((index + 1) % NUM_GPUS) as u8,
                target_expert_slot: (index % EXPERT_SLOTS_PER_GPU) as u8,
                payload_bytes: [0xAA; 64],
                lifecycle_state: AtomicU8::new(STATE_INIT),
            })
            .collect();

        let virtual_gpus = (0..NUM_GPUS).map(VirtualGpuDevice::new).collect();

        Self {
            cluster_nodes,
            active_pool,
            virtual_gpus,
        }
    }

    fn prepare_token(&self, index: usize) -> bool {
        let Some(token) = self.active_pool.get(index) else {
            return false;
        };

        token.lifecycle_state.store(STATE_PAGED, Ordering::Release);

        if let Some(destination_gpu) = self.virtual_gpus.get(token.destination_gpu_id as usize) {
            destination_gpu
                .write_expert_byte(token.target_expert_slot as usize, token.token_id as u8);
        } else {
            return false;
        }

        token
            .lifecycle_state
            .store(STATE_IN_FLIGHT, Ordering::Release);
        true
    }

    fn mark_token_complete(&self, index: usize) {
        if let Some(token) = self.active_pool.get(index) {
            token
                .lifecycle_state
                .store(STATE_COMPLETE, Ordering::Release);
        }
    }
}

struct WorkerJob {
    kernel: Arc<HolisticRoutingKernel>,
    start: usize,
    end: usize,
    completed_sender: crossbeam_channel::Sender<usize>,
}

struct WorkerPool {
    job_sender: Option<crossbeam_channel::Sender<WorkerJob>>,
    worker_handles: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    fn new(sender_socket: &UdpSocket, available_cores: &[core_affinity::CoreId]) -> Self {
        let (job_sender, job_receiver) = crossbeam_channel::bounded::<WorkerJob>(WORKER_COUNT);
        let mut worker_handles = Vec::with_capacity(WORKER_COUNT);

        for worker_id in 0..WORKER_COUNT {
            let worker_receiver = job_receiver.clone();
            let worker_socket = sender_socket
                .try_clone()
                .expect("Failed to clone sender socket");
            let target_core = available_cores[(worker_id + 1) % available_cores.len()];

            worker_handles.push(thread::spawn(move || {
                let mut send_buffer = SendBatchBuffer::new();
                if !core_affinity::set_for_current(target_core) {
                    eprintln!(
                        "Unable to pin compute worker {} to CPU core {:?}.",
                        worker_id, target_core.id
                    );
                }
                println!(
                    "--> Compute Worker Lane [{}] assigned to CPU Core [{}]",
                    worker_id, target_core.id
                );

                while let Ok(job) = worker_receiver.recv() {
                    for index in job.start..job.end {
                        job.kernel.prepare_token(index);
                    }
                    let successful_sends = send_token_range(
                        &job.kernel,
                        job.start,
                        job.end,
                        &worker_socket,
                        &mut send_buffer,
                    );
                    let _ = job.completed_sender.send(successful_sends);
                }
            }));
        }

        Self {
            job_sender: Some(job_sender),
            worker_handles,
        }
    }

    fn process_request(&self, kernel: Arc<HolisticRoutingKernel>, token_count: usize) -> usize {
        let job_count = WORKER_COUNT.min(token_count);
        if job_count == 0 {
            return 0;
        }

        let (completed_sender, completed_receiver) = crossbeam_channel::bounded(job_count);
        let tasks_per_job = token_count / job_count;
        let extra_tasks = token_count % job_count;
        let mut next_index = 0;

        for job_id in 0..job_count {
            let task_count = tasks_per_job + usize::from(job_id < extra_tasks);
            let end = next_index + task_count;
            let job = WorkerJob {
                kernel: Arc::clone(&kernel),
                start: next_index,
                end,
                completed_sender: completed_sender.clone(),
            };
            if self
                .job_sender
                .as_ref()
                .expect("Worker pool is shutting down")
                .send(job)
                .is_err()
            {
                break;
            }
            next_index = end;
        }

        drop(completed_sender);
        completed_receiver.iter().sum()
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        drop(self.job_sender.take());
        for worker in self.worker_handles.drain(..) {
            let _ = worker.join();
        }
    }
}

pub struct HolisticEngine {
    worker_pool: WorkerPool,
}

impl HolisticEngine {
    pub fn new(sender_socket: &UdpSocket, available_cores: &[core_affinity::CoreId]) -> Self {
        Self {
            worker_pool: WorkerPool::new(sender_socket, available_cores),
        }
    }

    pub fn process_request(&self, kernel: Arc<HolisticRoutingKernel>, token_count: usize) -> usize {
        self.worker_pool
            .process_request(kernel.clone(), token_count.min(kernel.active_pool.len()))
    }
}

#[cfg(target_os = "linux")]
struct SendBatchBuffer {
    vectors: Vec<libc::iovec>,
    messages: Vec<libc::mmsghdr>,
}

#[cfg(not(target_os = "linux"))]
struct SendBatchBuffer;

impl SendBatchBuffer {
    fn new() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self {
                vectors: Vec::with_capacity(RECEIVE_BATCH_SIZE),
                messages: Vec::with_capacity(RECEIVE_BATCH_SIZE),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self
        }
    }
}

#[cfg(target_os = "linux")]
fn send_token_range(
    kernel: &HolisticRoutingKernel,
    start: usize,
    end: usize,
    sender_socket: &UdpSocket,
    send_buffer: &mut SendBatchBuffer,
) -> usize {
    use std::os::fd::AsRawFd;

    let mut sent_count = 0usize;
    let mut batch_start = start;

    while batch_start < end {
        let batch_end = (batch_start + RECEIVE_BATCH_SIZE).min(end);
        send_buffer.messages.clear();
        send_buffer.vectors.clear();
        for index in batch_start..batch_end {
            let payload = &kernel.active_pool[index].payload_bytes;
            send_buffer.vectors.push(libc::iovec {
                iov_base: payload.as_ptr().cast_mut().cast(),
                iov_len: payload.len(),
            });
        }
        for vector in &mut send_buffer.vectors {
            send_buffer.messages.push(libc::mmsghdr {
                msg_hdr: libc::msghdr {
                    msg_name: std::ptr::null_mut(),
                    msg_namelen: 0,
                    msg_iov: vector,
                    msg_iovlen: 1,
                    msg_control: std::ptr::null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                },
                msg_len: 0,
            });
        }

        let sent = unsafe {
            libc::sendmmsg(
                sender_socket.as_raw_fd(),
                send_buffer.messages.as_mut_ptr(),
                send_buffer.messages.len() as libc::c_uint,
                0,
            )
        };

        if sent > 0 {
            let sent = sent as usize;
            for index in batch_start..batch_start + sent {
                kernel.mark_token_complete(index);
            }
            sent_count += sent;
            batch_start += sent;
            continue;
        }

        let error = std::io::Error::last_os_error();
        if error.kind() == ErrorKind::Interrupted {
            continue;
        }
        eprintln!("Batched UDP send failed: {error}");
        break;
    }

    sent_count
}

#[cfg(not(target_os = "linux"))]
fn send_token_range(
    kernel: &HolisticRoutingKernel,
    start: usize,
    end: usize,
    sender_socket: &UdpSocket,
    _send_buffer: &mut SendBatchBuffer,
) -> usize {
    let mut sent_count = 0;
    for index in start..end {
        if sender_socket
            .send(&kernel.active_pool[index].payload_bytes)
            .is_ok()
        {
            kernel.mark_token_complete(index);
            sent_count += 1;
        }
    }
    sent_count
}

#[cfg(target_os = "linux")]
fn receive_packets(receiver_socket: &UdpSocket, packet_target: usize) -> usize {
    use std::os::fd::AsRawFd;

    let mut packet_buffers = [[0u8; 64]; RECEIVE_BATCH_SIZE];
    let mut packets_received = 0usize;

    while packets_received < packet_target {
        let mut vectors: [libc::iovec; RECEIVE_BATCH_SIZE] =
            std::array::from_fn(|index| libc::iovec {
                iov_base: packet_buffers[index].as_mut_ptr().cast(),
                iov_len: packet_buffers[index].len(),
            });
        let mut messages: [libc::mmsghdr; RECEIVE_BATCH_SIZE] =
            std::array::from_fn(|index| libc::mmsghdr {
                msg_hdr: libc::msghdr {
                    msg_name: std::ptr::null_mut(),
                    msg_namelen: 0,
                    msg_iov: &mut vectors[index],
                    msg_iovlen: 1,
                    msg_control: std::ptr::null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                },
                msg_len: 0,
            });
        let mut timeout = libc::timespec {
            tv_sec: 3,
            tv_nsec: 0,
        };

        let received = unsafe {
            libc::recvmmsg(
                receiver_socket.as_raw_fd(),
                messages.as_mut_ptr(),
                RECEIVE_BATCH_SIZE as libc::c_uint,
                libc::MSG_WAITFORONE,
                &mut timeout,
            )
        };

        if received > 0 {
            packets_received += received as usize;
            continue;
        }
        if received == 0 {
            break;
        }

        let error = std::io::Error::last_os_error();
        if error.kind() == ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::TimedOut {
            break;
        }
        eprintln!("Batched UDP receive failed: {error}");
        break;
    }

    packets_received.min(packet_target)
}

#[cfg(not(target_os = "linux"))]
fn receive_packets(receiver_socket: &UdpSocket, packet_target: usize) -> usize {
    let mut packet_buffer = [0u8; 64];
    let mut packets_received = 0usize;

    while packets_received < packet_target {
        match receiver_socket.recv_from(&mut packet_buffer) {
            Ok((bytes_read, _source)) if bytes_read > 0 => packets_received += 1,
            Ok(_) => {}
            Err(error)
                if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::TimedOut =>
            {
                break;
            }
            Err(error) => {
                eprintln!("UDP receive failed: {error}");
                break;
            }
        }
    }

    packets_received
}

pub fn run_cli() {
    println!("===============================================================");
    println!("   LAUNCHING HOE ENGINE WITH HARDWARE CORE AFFINITY PINNING    ");
    println!("===============================================================");

    let available_cores = core_affinity::get_core_ids().unwrap_or_default();
    if available_cores.is_empty() {
        eprintln!("No CPU cores are available for affinity pinning.");
        return;
    }

    println!(
        "Detected system compute layer with {} logical cores.",
        available_cores.len()
    );

    let total_simulation_tokens = 50_000;
    let node_clusters = 8;
    let kernel = Arc::new(HolisticRoutingKernel::new(
        node_clusters,
        total_simulation_tokens,
    ));

    let sender_socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind sender socket");
    sender_socket
        .connect("127.0.0.1:9999")
        .expect("Failed to connect sender socket");
    let receiver_socket =
        UdpSocket::bind("127.0.0.1:9999").expect("Failed to bind receiver socket");
    if let Err(error) =
        socket2::SockRef::from(&receiver_socket).set_recv_buffer_size(RECEIVE_BUFFER_BYTES)
    {
        eprintln!("Unable to set UDP receive buffer size: {error}");
    }
    receiver_socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("Failed to set receiver socket timeout");

    let start_time = Instant::now();
    let receiver_core = available_cores[0];
    let network_listener_handle = thread::spawn(move || {
        if !core_affinity::set_for_current(receiver_core) {
            eprintln!(
                "Unable to pin network listener to CPU core {:?}.",
                receiver_core.id
            );
        }
        println!(
            "--> Network Driver Pipeline assigned to CPU Core [{}]",
            receiver_core.id
        );

        receive_packets(&receiver_socket, total_simulation_tokens)
    });

    let engine = HolisticEngine::new(&sender_socket, &available_cores);
    let successfully_sent = engine.process_request(Arc::clone(&kernel), total_simulation_tokens);

    let reported_network_packets = network_listener_handle.join().unwrap_or(0);
    let completed_tokens = kernel
        .active_pool
        .iter()
        .filter(|token| token.load_lifecycle_state() == STATE_COMPLETE)
        .count();
    let total_duration = start_time.elapsed();

    println!("\n=== PERFORMANCE ANALYTICS (THREAD-PINNED KERNEL) ===");
    println!(
        "Total Tokens Streamed Across Cluster : {}",
        total_simulation_tokens
    );
    println!(
        "Packets Intercepted via Pinned Core  : {}",
        reported_network_packets
    );
    println!(
        "Datagrams sent successfully          : {}",
        successfully_sent
    );
    println!(
        "Tokens in COMPLETE state             : {}",
        completed_tokens
    );
    println!(
        "Total Core Execution Processing Time : {:?}",
        total_duration
    );
    println!(
        "Average Transaction Latency          : {:?}",
        total_duration / total_simulation_tokens as u32
    );
    println!("===============================================================");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_pool_reuses_threads_for_multiple_requests() {
        let available_cores = core_affinity::get_core_ids().unwrap_or_default();
        assert!(!available_cores.is_empty());

        let receiver_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver_socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let receiver_address = receiver_socket.local_addr().unwrap();
        let receiver = thread::spawn(move || receive_packets(&receiver_socket, 8));

        let sender_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        sender_socket.connect(receiver_address).unwrap();
        let engine = HolisticEngine::new(&sender_socket, &available_cores);

        let first_request = Arc::new(HolisticRoutingKernel::new(1, 4));
        let second_request = Arc::new(HolisticRoutingKernel::new(1, 4));
        assert_eq!(engine.process_request(first_request, 4), 4);
        assert_eq!(engine.process_request(second_request, 4), 4);

        drop(engine);
        assert_eq!(receiver.join().unwrap(), 8);
    }
}
