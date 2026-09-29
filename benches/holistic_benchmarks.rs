use moe_holistic_engine::{HolisticEngine, HolisticRoutingKernel};
use std::net::UdpSocket;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const RECEIVE_BUFFER_BYTES: usize = 8 * 1024 * 1024;

fn main() {
    println!("===============================================================");
    println!("   HOE ENGINE BENCHMARK SUITE (VECTORIZED & CORE-PINNED)      ");
    println!("===============================================================");

    let cluster_nodes = 8;
    let tokens_per_batch = 10_000;
    let warmup_sweeps = 5;
    let measurement_sweeps = 20;
    let total_tokens = tokens_per_batch * (warmup_sweeps + measurement_sweeps);

    let available_cores = core_affinity::get_core_ids().unwrap_or_default();
    if available_cores.is_empty() {
        eprintln!("No CPU cores are available for affinity pinning.");
        return;
    }

    let receiver_socket =
        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind benchmark receiver socket");
    if let Err(error) =
        socket2::SockRef::from(&receiver_socket).set_recv_buffer_size(RECEIVE_BUFFER_BYTES)
    {
        eprintln!("Unable to set benchmark receive buffer size: {error}");
    }
    receiver_socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("Failed to set benchmark receiver timeout");
    let receiver_address = receiver_socket
        .local_addr()
        .expect("Failed to read benchmark receiver address");

    let receiver = thread::spawn(move || {
        let mut packet_buffer = [0u8; 64];
        let mut packets_received = 0usize;
        while packets_received < total_tokens {
            match receiver_socket.recv_from(&mut packet_buffer) {
                Ok((bytes_read, _)) if bytes_read > 0 => packets_received += 1,
                Ok(_) => {}
                Err(error)
                    if error.kind() == std::io::ErrorKind::TimedOut
                        || error.kind() == std::io::ErrorKind::WouldBlock =>
                {
                    break;
                }
                Err(error) => {
                    eprintln!("Benchmark receive failed: {error}");
                    break;
                }
            }
        }
        packets_received
    });

    let sender_socket =
        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind benchmark sender socket");
    sender_socket
        .connect(receiver_address)
        .expect("Failed to connect benchmark sender socket");

    let kernel = Arc::new(HolisticRoutingKernel::new(cluster_nodes, tokens_per_batch));
    let engine = HolisticEngine::new(&sender_socket, &available_cores);

    print!(
        "Warming up instruction cache ({} sweeps)... ",
        warmup_sweeps
    );
    let mut warmup_sent = 0usize;
    for _ in 0..warmup_sweeps {
        warmup_sent += engine.process_request(Arc::clone(&kernel), tokens_per_batch);
    }
    println!("Done.");

    println!(
        "Benchmarking {} sweeps ({} tokens/sweep)...",
        measurement_sweeps, tokens_per_batch
    );
    let start_time = Instant::now();
    let mut total_sent = 0usize;
    for _ in 0..measurement_sweeps {
        total_sent += engine.process_request(Arc::clone(&kernel), tokens_per_batch);
    }
    let total_elapsed = start_time.elapsed();

    let packets_received = receiver.join().unwrap_or(0);
    let all_sweeps_sent = warmup_sent + total_sent;
    let measured_tokens = tokens_per_batch * measurement_sweeps;
    let avg_sweep_duration = total_elapsed / measurement_sweeps as u32;
    let avg_token_latency = total_elapsed / measured_tokens as u32;
    let throughput_tps = measured_tokens as f64 / total_elapsed.as_secs_f64();

    println!("\n=== BENCHMARK PERFORMANCE METRICS ===");
    println!("Total Batched Tokens Streamed : {}", measured_tokens);
    println!("Datagrams sent (all sweeps)    : {}", all_sweeps_sent);
    println!("Datagrams received (all sweeps): {}", packets_received);
    println!("Total Measurement Duration     : {:?}", total_elapsed);
    println!("Mean Sweep Duration (10k Batch): {:?}", avg_sweep_duration);
    println!("Mean Transaction Latency/Token : {:?}", avg_token_latency);
    println!(
        "Engine Throughput              : {:.2} tokens/sec",
        throughput_tps
    );
    println!("===============================================================");
}
