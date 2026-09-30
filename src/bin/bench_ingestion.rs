use std::env;
use std::io;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const BATCH_SIZE: usize = 64;
const PACKET_BUF_SIZE: usize = 2048;
const PACKET_SIZE: usize = 64;
const RECEIVE_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const SOCKET_TIMEOUT: Duration = Duration::from_millis(50);
const DRAIN_QUIET_PERIOD: Duration = Duration::from_millis(150);

struct ReceiveStats {
    datagrams: u64,
    matching_packets: u64,
    bytes: u64,
    receive_calls: u64,
}

#[cfg(target_os = "linux")]
fn receive_batch(socket: &UdpSocket, messages: &mut [libc::mmsghdr]) -> io::Result<usize> {
    use std::os::fd::AsRawFd;

    loop {
        let received = unsafe {
            libc::recvmmsg(
                socket.as_raw_fd(),
                messages.as_mut_ptr(),
                messages.len() as libc::c_uint,
                libc::MSG_WAITFORONE,
                std::ptr::null_mut(),
            )
        };

        if received >= 0 {
            return Ok(received as usize);
        }

        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(error);
    }
}

#[cfg(target_os = "linux")]
fn send_batch(socket: &UdpSocket, messages: &mut [libc::mmsghdr]) -> io::Result<usize> {
    use std::os::fd::AsRawFd;

    loop {
        let sent = unsafe {
            libc::sendmmsg(
                socket.as_raw_fd(),
                messages.as_mut_ptr(),
                messages.len() as libc::c_uint,
                0,
            )
        };

        if sent >= 0 {
            return Ok(sent as usize);
        }

        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(error);
    }
}

fn start_loopback_generator(
    receiver_address: std::net::SocketAddr,
    duration: Duration,
    started_at: Instant,
    finished: Arc<AtomicBool>,
) -> thread::JoinHandle<io::Result<u64>> {
    thread::spawn(move || {
        let socket = UdpSocket::bind("127.0.0.1:0")?;
        socket.connect(receiver_address)?;

        let mut payloads = [[0x77u8; PACKET_SIZE]; BATCH_SIZE];
        let mut vectors: Vec<libc::iovec> = payloads
            .iter_mut()
            .map(|payload| libc::iovec {
                iov_base: payload.as_mut_ptr().cast(),
                iov_len: payload.len(),
            })
            .collect();
        let mut messages: Vec<libc::mmsghdr> = vectors
            .iter_mut()
            .map(|vector| libc::mmsghdr {
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
            })
            .collect();

        let mut packets_sent = 0u64;
        while started_at.elapsed() < duration {
            match send_batch(&socket, &mut messages) {
                Ok(0) => thread::yield_now(),
                Ok(sent) => packets_sent += sent as u64,
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::Interrupted =>
                {
                    thread::yield_now();
                }
                Err(error) => {
                    finished.store(true, Ordering::Release);
                    return Err(error);
                }
            }
        }

        finished.store(true, Ordering::Release);
        Ok(packets_sent)
    })
}

fn run_recvmmsg_benchmark(
    bind_address: &str,
    duration: Duration,
    self_test: bool,
) -> io::Result<()> {
    let socket = UdpSocket::bind(bind_address)?;
    socket.set_read_timeout(Some(SOCKET_TIMEOUT))?;
    if let Err(error) = socket2::SockRef::from(&socket).set_recv_buffer_size(RECEIVE_BUFFER_BYTES) {
        eprintln!("Could not set receive buffer to {RECEIVE_BUFFER_BYTES} bytes: {error}");
    }
    let receiver_address = socket.local_addr()?;

    let started_at = Instant::now();
    let sender_finished = Arc::new(AtomicBool::new(!self_test));
    let sender = self_test.then(|| {
        start_loopback_generator(
            receiver_address,
            duration,
            started_at,
            Arc::clone(&sender_finished),
        )
    });

    println!("[recvmmsg] Listening on {receiver_address} for {duration:?}.");
    if self_test {
        println!("[recvmmsg] Sending 64-byte 0x77 loopback datagrams in batches of {BATCH_SIZE}.");
    } else {
        println!(
            "[recvmmsg] External traffic mode; send 0x77-prefixed UDP payloads to this socket."
        );
    }

    let mut buffers = vec![[0u8; PACKET_BUF_SIZE]; BATCH_SIZE];
    let mut vectors: Vec<libc::iovec> = buffers
        .iter_mut()
        .map(|buffer| libc::iovec {
            iov_base: buffer.as_mut_ptr().cast(),
            iov_len: buffer.len(),
        })
        .collect();
    let mut messages: Vec<libc::mmsghdr> = vectors
        .iter_mut()
        .map(|vector| libc::mmsghdr {
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
        })
        .collect();
    let mut stats = ReceiveStats {
        datagrams: 0,
        matching_packets: 0,
        bytes: 0,
        receive_calls: 0,
    };
    let mut last_packet_at = started_at;

    loop {
        let elapsed = started_at.elapsed();
        let sender_done = sender_finished.load(Ordering::Acquire);
        let duration_elapsed = elapsed >= duration;
        if duration_elapsed && sender_done && last_packet_at.elapsed() >= DRAIN_QUIET_PERIOD {
            break;
        }

        match receive_batch(&socket, &mut messages) {
            Ok(0) => {}
            Ok(received) => {
                stats.receive_calls += 1;
                last_packet_at = Instant::now();
                stats.datagrams += received as u64;
                for index in 0..received {
                    let bytes_read = messages[index].msg_len as usize;
                    stats.bytes += bytes_read as u64;
                    if bytes_read > 0 && buffers[index][0] == 0x77 {
                        stats.matching_packets += 1;
                    }
                }
            }
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut =>
            {
                if !self_test && started_at.elapsed() >= duration {
                    break;
                }
            }
            Err(error) => return Err(error),
        }

        if started_at.elapsed() >= duration && !self_test {
            break;
        }
    }

    let packets_sent = match sender {
        Some(handle) => handle
            .join()
            .map_err(|_| io::Error::other("loopback generator panicked"))??,
        None => 0,
    };

    let elapsed = started_at.elapsed().as_secs_f64();
    let packets_per_second = stats.datagrams as f64 / elapsed;
    let gigabits_per_second = stats.bytes as f64 * 8.0 / elapsed / 1_000_000_000.0;
    let average_batch = if stats.receive_calls == 0 {
        0.0
    } else {
        stats.datagrams as f64 / stats.receive_calls as f64
    };

    println!("\n=== INGESTION BENCHMARK ===");
    println!("Datagrams sent (self-test) : {packets_sent}");
    println!("Datagrams received         : {}", stats.datagrams);
    println!("0x77 packets               : {}", stats.matching_packets);
    println!("Payload bytes received     : {}", stats.bytes);
    println!("Successful recvmmsg calls  : {}", stats.receive_calls);
    println!("Average datagrams per call : {average_batch:.2}");
    println!("Elapsed time               : {:.3}s", elapsed);
    println!(
        "Receive throughput         : {:.2} Kpps",
        packets_per_second / 1_000.0
    );
    println!("Payload bitrate            : {gigabits_per_second:.4} Gbit/s");
    if self_test && packets_sent > 0 {
        println!(
            "Observed loopback delivery : {:.2}%",
            stats.datagrams as f64 * 100.0 / packets_sent as f64
        );
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn unsupported_platform() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "the recvmmsg benchmark currently requires Linux",
    ))
}

fn main() -> io::Result<()> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    let bind_address = arguments
        .first()
        .map(String::as_str)
        .unwrap_or("127.0.0.1:9000");
    let seconds = arguments
        .get(1)
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        .unwrap_or(3);
    let self_test = arguments.get(2).map(String::as_str) != Some("--external");

    if seconds == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "duration must be at least one second",
        ));
    }

    #[cfg(target_os = "linux")]
    return run_recvmmsg_benchmark(bind_address, Duration::from_secs(seconds), self_test);

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (bind_address, seconds, self_test);
        unsupported_platform()
    }
}
