use std::env;
use std::io;
use std::net::UdpSocket;
use std::thread;
use std::time::{Duration, Instant};

const BATCH_SIZE: usize = 64;
const TOKEN_HEADER_SIZE: usize = 10;

#[derive(Clone, Copy)]
enum Distribution {
    Uniform,
    Skewed { hot_expert: u8, hot_percent: u8 },
}

struct WorkerResult {
    packets: u64,
    bytes: u64,
}

#[cfg(target_os = "linux")]
fn send_messages(socket: &UdpSocket, messages: &mut [libc::mmsghdr]) -> io::Result<usize> {
    use std::os::fd::AsRawFd;

    loop {
        let result = unsafe {
            libc::sendmmsg(
                socket.as_raw_fd(),
                messages.as_mut_ptr(),
                messages.len() as libc::c_uint,
                0,
            )
        };
        if result >= 0 {
            return Ok(result as usize);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn generate_expert(distribution: Distribution, sequence: u64, expert_count: u8) -> u8 {
    match distribution {
        Distribution::Uniform => (sequence % u64::from(expert_count)) as u8,
        Distribution::Skewed {
            hot_expert,
            hot_percent,
        } if sequence % 100 < u64::from(hot_percent) => hot_expert,
        Distribution::Skewed { hot_expert, .. } if expert_count > 1 => {
            let other = (sequence % u64::from(expert_count - 1)) as u8;
            if other >= hot_expert {
                other + 1
            } else {
                other
            }
        }
        Distribution::Skewed { hot_expert, .. } => hot_expert,
    }
}

fn run_worker(
    worker_id: usize,
    target: String,
    duration: Duration,
    expert_count: u8,
    distribution: Distribution,
    worker_pps: Option<u64>,
    core: Option<core_affinity::CoreId>,
) -> io::Result<WorkerResult> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect(target)?;
    if let Some(core) = core {
        if !core_affinity::set_for_current(core) {
            eprintln!("Worker {worker_id} could not pin to CPU core {}", core.id);
        }
    }

    let mut payloads = [[0u8; TOKEN_HEADER_SIZE]; BATCH_SIZE];
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

    let started_at = Instant::now();
    let mut packet_count = 0u64;
    let mut bytes_count = 0u64;
    let sequence_base = (worker_id as u64) << 48;

    if worker_pps == Some(0) {
        return Ok(WorkerResult {
            packets: 0,
            bytes: 0,
        });
    }

    while started_at.elapsed() < duration {
        for index in 0..BATCH_SIZE {
            let sequence = sequence_base + packet_count + index as u64 + 1;
            payloads[index][0] = 0x77;
            payloads[index][1..9].copy_from_slice(&sequence.to_be_bytes());
            payloads[index][9] = generate_expert(distribution, sequence, expert_count);
            messages[index].msg_len = 0;
        }

        let mut batch_offset = 0;
        while batch_offset < BATCH_SIZE && started_at.elapsed() < duration {
            let sent = send_messages(&socket, &mut messages[batch_offset..])?;
            if sent == 0 {
                thread::yield_now();
                continue;
            }
            batch_offset += sent;
            packet_count += sent as u64;
            bytes_count += (sent * TOKEN_HEADER_SIZE) as u64;
        }

        if let Some(worker_pps) = worker_pps {
            let target_elapsed = Duration::from_secs_f64(packet_count as f64 / worker_pps as f64);
            if let Some(wait) = target_elapsed.checked_sub(started_at.elapsed()) {
                thread::sleep(wait);
            }
        }
    }

    Ok(WorkerResult {
        packets: packet_count,
        bytes: bytes_count,
    })
}

fn parse<T: std::str::FromStr>(value: Option<&String>, default: T, name: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value.map_or(Ok(default), |text| {
        text.parse()
            .map_err(|error| format!("invalid {name}: {error}"))
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_os = "linux"))]
    return Err("traffic_profiler requires Linux sendmmsg".into());

    #[cfg(target_os = "linux")]
    {
        let arguments: Vec<String> = env::args().skip(1).collect();
        if arguments.len() < 2 {
            return Err("usage: traffic_profiler <target-addr> <threads> [seconds] [experts] [uniform|skewed] [hot-percent] [total-pps]".into());
        }

        let target = arguments[0].clone();
        let worker_count: usize = parse(arguments.get(1), 0, "thread count")?;
        let seconds: u64 = parse(arguments.get(2), 5, "duration")?;
        let expert_count: u8 = parse(arguments.get(3), 8, "expert count")?;
        let strategy = arguments.get(4).map(String::as_str).unwrap_or("skewed");
        let hot_percent: u8 = parse(arguments.get(5), 70, "hot percentage")?;
        let total_pps: u64 = parse(arguments.get(6), 0, "packet rate")?;

        if worker_count == 0 || seconds == 0 || expert_count == 0 || hot_percent > 100 {
            return Err(
                "threads, seconds, and experts must be positive; hot percentage must be 0..=100"
                    .into(),
            );
        }
        let distribution = match strategy {
            "uniform" => Distribution::Uniform,
            "skewed" => Distribution::Skewed {
                hot_expert: 0,
                hot_percent,
            },
            _ => return Err("distribution must be uniform or skewed".into()),
        };

        let cores = core_affinity::get_core_ids().unwrap_or_default();
        println!(
            "Generating {strategy} UDP token traffic to {target} with {worker_count} workers for {seconds}s"
        );
        println!(
            "Rate limit: {}",
            if total_pps == 0 {
                "unlimited".to_owned()
            } else {
                format!("{total_pps} packets/s")
            }
        );

        let duration = Duration::from_secs(seconds);
        let mut handles = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let core = cores.get(worker_id % cores.len().max(1)).copied();
            let worker_pps = if total_pps == 0 {
                None
            } else {
                Some(
                    total_pps / worker_count as u64
                        + u64::from((worker_id as u64) < total_pps % worker_count as u64),
                )
            };
            handles.push(thread::spawn({
                let target = target.clone();
                move || {
                    run_worker(
                        worker_id,
                        target,
                        duration,
                        expert_count,
                        distribution,
                        worker_pps,
                        core,
                    )
                }
            }));
        }

        let mut total_packets = 0u64;
        let mut total_bytes = 0u64;
        for handle in handles {
            let result = handle
                .join()
                .map_err(|_| io::Error::other("traffic worker panicked"))??;
            total_packets += result.packets;
            total_bytes += result.bytes;
        }

        let elapsed = seconds as f64;
        println!("Packets sent : {total_packets}");
        println!(
            "Payload MiB  : {:.2}",
            total_bytes as f64 / (1024.0 * 1024.0)
        );
        println!(
            "Average rate : {:.2} Kpps",
            total_packets as f64 / elapsed / 1_000.0
        );
        println!(
            "Average data : {:.4} Gbit/s",
            total_bytes as f64 * 8.0 / elapsed / 1_000_000_000.0
        );
        Ok(())
    }
}
