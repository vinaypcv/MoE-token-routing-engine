use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const BATCH_SIZE: usize = 64;
const TOKEN_HEADER_SIZE: usize = 14;
const MAX_UDP_PAYLOAD_SIZE: usize = 65_507;
const ZIPF_LUT_SIZE: usize = 65_536;
const HISTOGRAM_LOWEST_NS: u64 = 100;
const HISTOGRAM_HIGHEST_NS: u64 = 10_000_000_000;
const HISTOGRAM_PRECISION: f64 = 1.01;

#[derive(Clone, Copy)]
enum Distribution {
    Uniform,
    Skewed { hot_expert: u8, hot_percent: u8 },
    Zipf,
}

struct WorkerResult {
    packets: u64,
    bytes: u64,
    send_latency: LatencyHistogram,
    expert_counts: [u64; 8],
}

struct WorkerConfig {
    worker_id: usize,
    target: String,
    duration: Duration,
    expert_count: u8,
    distribution: Distribution,
    worker_pps: Option<u64>,
    feature_bytes: usize,
    embed_t0: bool,
    core: Option<core_affinity::CoreId>,
    zipf_sampler: Option<Arc<Mutex<ZipfSampler>>>,
    expert_sequences: Arc<[AtomicU32; 8]>,
    ordered_batch_lock: Arc<Mutex<()>>,
}

struct ZipfSampler {
    lut: Vec<u32>,
    rng_state: u64,
}

impl ZipfSampler {
    fn new(num_experts: u32, skew: f64, seed: u64) -> Result<Self, String> {
        if num_experts == 0 || !skew.is_finite() || skew < 0.0 {
            return Err("Zipf sampling requires experts > 0 and finite skew >= 0".into());
        }

        let mut cdf = Vec::with_capacity(num_experts as usize);
        let mut total = 0.0;
        for rank in 1..=num_experts {
            total += f64::from(rank).powf(-skew);
            cdf.push(total);
        }

        let mut lut = Vec::with_capacity(ZIPF_LUT_SIZE);
        let mut rank_index = 0;
        for slot in 0..ZIPF_LUT_SIZE {
            let probability = ((slot as f64 + 0.5) / ZIPF_LUT_SIZE as f64) * total;
            while rank_index + 1 < cdf.len() && cdf[rank_index] < probability {
                rank_index += 1;
            }
            lut.push(rank_index as u32);
        }

        Ok(Self {
            lut,
            rng_state: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
        })
    }

    #[inline]
    fn next_expert(&mut self) -> u32 {
        let mut value = self.rng_state;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        self.rng_state = value;
        let random = value.wrapping_mul(0x2545_f491_4f6c_dd1d);
        let index = (random >> 48) as usize;
        self.lut[index]
    }
}

struct LatencyHistogram {
    upper_bounds_ns: Vec<u64>,
    counts: Vec<u64>,
    sample_count: u64,
}

impl LatencyHistogram {
    fn new() -> Self {
        let mut upper_bounds_ns = vec![HISTOGRAM_LOWEST_NS];
        while *upper_bounds_ns
            .last()
            .expect("histogram starts with a bound")
            < HISTOGRAM_HIGHEST_NS
        {
            let previous = *upper_bounds_ns.last().expect("histogram has a bound");
            let next = ((previous as f64 * HISTOGRAM_PRECISION).ceil() as u64)
                .max(previous + 1)
                .min(HISTOGRAM_HIGHEST_NS);
            upper_bounds_ns.push(next);
        }
        let counts = vec![0; upper_bounds_ns.len()];
        Self {
            upper_bounds_ns,
            counts,
            sample_count: 0,
        }
    }

    fn record(&mut self, latency: Duration) {
        let nanos = latency.as_nanos().min(u128::from(u64::MAX)) as u64;
        let index = self.upper_bounds_ns.partition_point(|bound| *bound < nanos);
        let index = index.min(self.counts.len() - 1);
        self.counts[index] += 1;
        self.sample_count += 1;
    }

    fn merge(&mut self, other: Self) {
        debug_assert_eq!(self.upper_bounds_ns, other.upper_bounds_ns);
        for (count, other_count) in self.counts.iter_mut().zip(other.counts) {
            *count += other_count;
        }
        self.sample_count += other.sample_count;
    }

    fn percentile_upper_bound(&self, percentile: f64) -> Option<u64> {
        if self.sample_count == 0 {
            return None;
        }
        let rank = (percentile * self.sample_count as f64).ceil().max(1.0) as u64;
        let mut cumulative = 0;
        self.counts.iter().enumerate().find_map(|(index, count)| {
            cumulative += count;
            (cumulative >= rank).then_some(self.upper_bounds_ns[index])
        })
    }
}

#[derive(Default)]
struct ReceiverMetrics {
    received_packets: u64,
    application_drops: u64,
    primary_expert_overflows: u64,
    fallback_routed: u64,
    max_expert_queue_depth: u64,
    execution_latency_buckets: Vec<(u64, u64)>,
}

impl ReceiverMetrics {
    fn counter_delta(mut self, before: Self) -> Self {
        self.received_packets = self
            .received_packets
            .saturating_sub(before.received_packets);
        self.application_drops = self
            .application_drops
            .saturating_sub(before.application_drops);
        self.primary_expert_overflows = self
            .primary_expert_overflows
            .saturating_sub(before.primary_expert_overflows);
        self.fallback_routed = self.fallback_routed.saturating_sub(before.fallback_routed);
        for (bucket, count) in self.execution_latency_buckets.iter_mut() {
            if let Some((_, before_count)) = before
                .execution_latency_buckets
                .iter()
                .find(|(before_bucket, _)| before_bucket == bucket)
            {
                *count = count.saturating_sub(*before_count);
            }
        }
        self
    }

    fn execution_percentile_ns(&self, percentile: f64) -> Option<u64> {
        let sample_count = self
            .execution_latency_buckets
            .iter()
            .map(|(_, count)| *count)
            .max()?;
        if sample_count == 0 {
            return None;
        }
        let rank = (sample_count as f64 * percentile).ceil().max(1.0) as u64;
        self.execution_latency_buckets
            .iter()
            .find(|(_, cumulative_count)| *cumulative_count >= rank)
            .map(|(upper_bound_ns, _)| *upper_bound_ns)
    }
}

fn fetch_prometheus_metrics(url: &str) -> io::Result<String> {
    let address = url.strip_prefix("http://").ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "metrics URL must use http://")
    })?;
    let (authority, path) = address.split_once('/').unwrap_or((address, "metrics"));
    let socket_address = authority.to_socket_addrs()?.next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "metrics host did not resolve")
    })?;
    let mut stream = TcpStream::connect_timeout(&socket_address, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    write!(
        stream,
        "GET /{path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (headers, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "invalid metrics HTTP response")
    })?;
    if !headers
        .lines()
        .next()
        .is_some_and(|status| status.contains(" 200 "))
    {
        return Err(io::Error::other(
            "metrics endpoint returned a non-200 status",
        ));
    }
    Ok(body.to_owned())
}

fn metric_value(metrics: &str, metric_name: &str) -> Option<u64> {
    metrics.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == metric_name).then(|| value.split_whitespace().next()?.parse().ok())?
    })
}

fn scrape_receiver_metrics(metrics: &str) -> ReceiverMetrics {
    let mut snapshot = ReceiverMetrics {
        received_packets: metric_value(metrics, "moe_rx_packets_total").unwrap_or(0),
        application_drops: metric_value(metrics, "moe_saturated_drops_total").unwrap_or(0)
            + metric_value(metrics, "moe_invalid_packets_total").unwrap_or(0)
            + metric_value(metrics, "moe_closed_queue_drops_total").unwrap_or(0),
        primary_expert_overflows: metrics
            .lines()
            .find(|line| line.starts_with("moe_expert_drops_total{expert_id=\"0\"}"))
            .and_then(|line| {
                line.rsplit_once(' ')
                    .and_then(|(_, value)| value.parse().ok())
            })
            .unwrap_or(0),
        fallback_routed: metric_value(metrics, "moe_fallback_routed_total").unwrap_or(0),
        ..ReceiverMetrics::default()
    };
    for line in metrics.lines() {
        if let Some(bucket) = line.strip_prefix("moe_ingress_to_completion_latency_seconds_bucket{")
        {
            if let Some((label, value)) = bucket.split_once("} ") {
                if let Some(bound_seconds) = label
                    .strip_prefix("le=\"")
                    .and_then(|value| value.strip_suffix('\"'))
                    .and_then(|value| value.parse::<f64>().ok())
                {
                    if let Ok(count) = value.parse::<u64>() {
                        snapshot
                            .execution_latency_buckets
                            .push(((bound_seconds * 1_000_000_000.0) as u64, count));
                    }
                }
            }
        }
        if !line.starts_with("moe_expert_queue_depth{") {
            continue;
        }
        if let Some((_, value)) = line.rsplit_once("} ") {
            if let Ok(depth) = value.parse::<u64>() {
                snapshot.max_expert_queue_depth = snapshot.max_expert_queue_depth.max(depth);
            }
        }
    }
    snapshot
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

fn generate_expert(
    distribution: Distribution,
    sequence: u64,
    expert_count: u8,
    zipf_sampler: Option<&mut ZipfSampler>,
) -> u8 {
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
        Distribution::Zipf => zipf_sampler
            .expect("Zipf distribution has a configured sampler")
            .next_expert() as u8,
    }
}

fn run_worker(config: WorkerConfig) -> io::Result<WorkerResult> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect(config.target)?;
    if let Some(core) = config.core {
        if !core_affinity::set_for_current(core) {
            eprintln!(
                "Worker {} could not pin to CPU core {}",
                config.worker_id, core.id
            );
        }
    }

    let packet_size = TOKEN_HEADER_SIZE + config.feature_bytes;
    let mut payloads: Vec<Vec<u8>> = (0..BATCH_SIZE).map(|_| vec![0u8; packet_size]).collect();
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
    let mut send_latency = LatencyHistogram::new();
    let mut expert_counts = [0u64; 8];
    let mut batch_experts = [0usize; BATCH_SIZE];
    let sequence_base = (config.worker_id as u64) << 48;

    if config.worker_pps == Some(0) {
        return Ok(WorkerResult {
            packets: 0,
            bytes: 0,
            send_latency,
            expert_counts,
        });
    }

    while started_at.elapsed() < config.duration {
        let _sequence_order = config
            .ordered_batch_lock
            .lock()
            .map_err(|_| io::Error::other("sequence ordering lock poisoned"))?;
        let mut zipf_sampler = config
            .zipf_sampler
            .as_ref()
            .map(|sampler| sampler.lock())
            .transpose()
            .map_err(|_| io::Error::other("Zipf sampler lock poisoned"))?;
        for index in 0..BATCH_SIZE {
            let sequence = sequence_base + packet_count + index as u64 + 1;
            payloads[index][0] = 0x77;
            payloads[index][1..9].copy_from_slice(&sequence.to_be_bytes());
            payloads[index][9] = generate_expert(
                config.distribution,
                sequence,
                config.expert_count,
                zipf_sampler.as_deref_mut(),
            );
            let expert_id = payloads[index][9] as usize;
            batch_experts[index] = expert_id;
            let sequence_id = config.expert_sequences[expert_id]
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1);
            payloads[index][10..14].copy_from_slice(&sequence_id.to_be_bytes());
            for (feature_index, activation) in
                payloads[index][TOKEN_HEADER_SIZE..].iter_mut().enumerate()
            {
                *activation = sequence.wrapping_add(feature_index as u64) as u8;
            }
            messages[index].msg_len = 0;
        }
        drop(zipf_sampler);

        let mut batch_offset = 0;
        let mut batch_timestamped = false;
        while batch_offset < BATCH_SIZE {
            if config.embed_t0 && !batch_timestamped {
                let timestamp_ns = moe_holistic_engine::engine::clock::measurement_now_ns()?;
                let timestamp_bytes = timestamp_ns.to_be_bytes();
                for payload in payloads.iter_mut().take(BATCH_SIZE) {
                    payload[TOKEN_HEADER_SIZE..TOKEN_HEADER_SIZE + 8]
                        .copy_from_slice(&timestamp_bytes);
                }
                batch_timestamped = true;
            }
            let send_started_at = Instant::now();
            let sent = send_messages(&socket, &mut messages[batch_offset..])?;
            send_latency.record(send_started_at.elapsed());
            if sent == 0 {
                thread::yield_now();
                continue;
            }
            batch_offset += sent;
            for expert_id in &batch_experts[batch_offset - sent..batch_offset] {
                expert_counts[*expert_id] += 1;
            }
            packet_count += sent as u64;
            bytes_count += (sent * packet_size) as u64;
        }
        drop(_sequence_order);

        if let Some(worker_pps) = config.worker_pps {
            let target_elapsed = Duration::from_secs_f64(packet_count as f64 / worker_pps as f64);
            if let Some(wait) = target_elapsed.checked_sub(started_at.elapsed()) {
                thread::sleep(wait);
            }
        }
    }

    Ok(WorkerResult {
        packets: packet_count,
        bytes: bytes_count,
        send_latency,
        expert_counts,
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
            return Err("usage: traffic_profiler <target-addr> <threads> [seconds] [experts] [uniform|skewed|zipf] [hot-percent] [total-pps] [feature-bytes] [zipf-skew] [seed]".into());
        }

        let target = arguments[0].clone();
        let worker_count: usize = parse(arguments.get(1), 0, "thread count")?;
        let seconds: u64 = parse(arguments.get(2), 5, "duration")?;
        let expert_count: u8 = parse(arguments.get(3), 8, "expert count")?;
        let strategy = arguments.get(4).map(String::as_str).unwrap_or("skewed");
        let hot_percent: u8 = parse(arguments.get(5), 70, "hot percentage")?;
        let total_pps: u64 = parse(arguments.get(6), 0, "packet rate")?;
        let feature_bytes: usize = parse(arguments.get(7), 256, "feature byte count")?;
        let zipf_skew: f64 = parse(arguments.get(8), 1.5, "Zipf skew")?;
        let seed: u64 = parse(arguments.get(9), 1, "Zipf seed")?;
        let embed_t0 = env::var("MOE_EMBED_T0")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
            .unwrap_or(false);

        if worker_count == 0
            || seconds == 0
            || expert_count == 0
            || expert_count > 8
            || hot_percent > 100
            || (embed_t0 && feature_bytes < 8)
            || TOKEN_HEADER_SIZE + feature_bytes > MAX_UDP_PAYLOAD_SIZE
        {
            return Err(
                "threads and seconds must be positive; experts must be 1..=8; hot percentage must be 0..=100; timestamp embedding requires at least 8 feature bytes; UDP payload must fit the IPv4 datagram limit"
                    .into(),
            );
        }
        let distribution = match strategy {
            "uniform" => Distribution::Uniform,
            "skewed" => Distribution::Skewed {
                hot_expert: 0,
                hot_percent,
            },
            "zipf" => Distribution::Zipf,
            _ => return Err("distribution must be uniform, skewed, or zipf".into()),
        };
        let zipf_sampler = if matches!(distribution, Distribution::Zipf) {
            Some(Arc::new(Mutex::new(ZipfSampler::new(
                u32::from(expert_count),
                zipf_skew,
                seed,
            )?)))
        } else {
            None
        };
        let receiver_metrics_before = if env::var("MOE_PROFILE_JSON").is_ok() {
            env::var("MOE_METRICS_URL")
                .ok()
                .map(|url| {
                    fetch_prometheus_metrics(&url).map(|body| scrape_receiver_metrics(&body))
                })
                .transpose()?
        } else {
            None
        };

        let cores = core_affinity::get_core_ids().unwrap_or_default();
        println!(
            "Generating {strategy} UDP tokens ({feature_bytes} feature bytes each) to {target} with {worker_count} workers for {seconds}s"
        );
        println!(
            "Rate limit: {}",
            if total_pps == 0 {
                "unlimited".to_owned()
            } else {
                format!("{total_pps} packets/s")
            }
        );
        if matches!(distribution, Distribution::Zipf) {
            println!("Zipf skew: {zipf_skew}; deterministic seed: {seed}");
        }

        let duration = Duration::from_secs(seconds);
        let expert_sequences = Arc::new(std::array::from_fn(|_| AtomicU32::new(0)));
        let ordered_batch_lock = Arc::new(Mutex::new(()));
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
                let zipf_sampler = zipf_sampler.clone();
                let expert_sequences = Arc::clone(&expert_sequences);
                let ordered_batch_lock = Arc::clone(&ordered_batch_lock);
                move || {
                    run_worker(WorkerConfig {
                        worker_id,
                        target,
                        duration,
                        expert_count,
                        distribution,
                        worker_pps,
                        feature_bytes,
                        embed_t0,
                        core,
                        zipf_sampler,
                        expert_sequences,
                        ordered_batch_lock,
                    })
                }
            }));
        }

        let mut total_packets = 0u64;
        let mut total_bytes = 0u64;
        let mut send_latency = LatencyHistogram::new();
        let mut expert_counts = [0u64; 8];
        for handle in handles {
            let result = handle
                .join()
                .map_err(|_| io::Error::other("traffic worker panicked"))??;
            total_packets += result.packets;
            total_bytes += result.bytes;
            send_latency.merge(result.send_latency);
            for (total, worker_count) in expert_counts.iter_mut().zip(result.expert_counts) {
                *total += worker_count;
            }
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
        println!(
            "sendmmsg call latency upper bounds (ns): p50={} p99={} p99.9={}",
            send_latency.percentile_upper_bound(0.50).unwrap_or(0),
            send_latency.percentile_upper_bound(0.99).unwrap_or(0),
            send_latency.percentile_upper_bound(0.999).unwrap_or(0)
        );

        if let Ok(report_path) = env::var("MOE_PROFILE_JSON") {
            let pattern = match strategy {
                "uniform" => "uniform",
                "skewed" => "skewed",
                _ => "zipf",
            };
            let skew_json = if pattern == "zipf" {
                zipf_skew.to_string()
            } else {
                "null".to_owned()
            };
            let target_rate_json = if total_pps == 0 {
                "null".to_owned()
            } else {
                total_pps.to_string()
            };
            let counts_json = expert_counts
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let receiver_metrics_after = env::var("MOE_METRICS_URL")
                .ok()
                .map(|url| {
                    fetch_prometheus_metrics(&url).map(|body| scrape_receiver_metrics(&body))
                })
                .transpose()?;
            let receiver_metrics =
                receiver_metrics_after.map(|after| match receiver_metrics_before {
                    Some(before) => after.counter_delta(before),
                    None => after,
                });
            let receiver_queue_capacity = env::var("MOE_DEMO_QUEUE_CAPACITY")
                .ok()
                .map(|value| value.parse::<u64>())
                .transpose()?;
            let received_json = receiver_metrics
                .as_ref()
                .map(|metrics| metrics.received_packets.to_string())
                .unwrap_or_else(|| "null".to_owned());
            let dropped_json = receiver_metrics
                .as_ref()
                .map(|metrics| {
                    total_packets
                        .saturating_sub(metrics.received_packets)
                        .saturating_add(metrics.application_drops)
                        .to_string()
                })
                .unwrap_or_else(|| "null".to_owned());
            let max_depth_pct_json = receiver_metrics
                .as_ref()
                .map(|metrics| match receiver_queue_capacity {
                    Some(capacity) if capacity > 0 => format!(
                        "{:.2}",
                        metrics.max_expert_queue_depth as f64 * 100.0 / capacity as f64
                    ),
                    _ => "null".to_owned(),
                })
                .unwrap_or_else(|| "null".to_owned());
            let primary_overflows_json = receiver_metrics
                .as_ref()
                .map(|metrics| metrics.primary_expert_overflows.to_string())
                .unwrap_or_else(|| "null".to_owned());
            let fallback_routed_json = receiver_metrics
                .as_ref()
                .map(|metrics| metrics.fallback_routed.to_string())
                .unwrap_or_else(|| "null".to_owned());
            let execution_percentile_json = |percentile| {
                receiver_metrics
                    .as_ref()
                    .and_then(|metrics| metrics.execution_percentile_ns(percentile))
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_owned())
            };
            let lossless_delivery_json = receiver_metrics
                .as_ref()
                .map(|metrics| {
                    (total_packets == metrics.received_packets && metrics.application_drops == 0)
                        .to_string()
                })
                .unwrap_or_else(|| "null".to_owned());
            let report = format!(
                "{{\n  \"schema_version\": 1,\n  \"measurement_scope\": \"sendmmsg syscall latency and sender CLOCK_MONOTONIC T0 through receiver expert worker completion\",\n  \"tail_latency_claim_eligible\": false,\n  \"workload\": {{\n    \"pattern\": \"{pattern}\",\n    \"skew_s\": {skew_json},\n    \"seed\": {seed},\n    \"total_tokens\": {total_packets},\n    \"target_rate_pps\": {target_rate_json}\n  }},\n  \"queue_telemetry\": {{\n    \"max_queue_depth_pct\": null,\n    \"current_queue_depth_pct_at_scrape\": {max_depth_pct_json},\n    \"time_above_90pct_ms\": null,\n    \"primary_expert_overflows\": {primary_overflows_json},\n    \"fallback_quant_routed\": {fallback_routed_json},\n    \"observed_expert_queue_depth_max_at_scrape\": {}\n  }},\n  \"traffic_counters\": {{\n    \"sent_packets\": {total_packets},\n    \"received_packets\": {received_json},\n    \"dropped_packets\": {dropped_json},\n    \"lossless_delivery_observed\": {lossless_delivery_json}\n  }},\n  \"latency_nanoseconds\": {{\n    \"sender_boundary\": \"duration of each sendmmsg call\",\n    \"sender_p50\": {},\n    \"sender_p99\": {},\n    \"sender_p99_9\": {},\n    \"execution_boundary\": \"sender CLOCK_MONOTONIC T0 immediately before frame send through receiver worker completion T1\",\n    \"execution_p50\": {},\n    \"execution_p99\": {},\n    \"execution_p99_9\": {}\n  }},\n  \"expert_token_counts\": [{counts_json}]\n}}\n",
                receiver_metrics
                    .as_ref()
                    .map(|metrics| metrics.max_expert_queue_depth.to_string())
                    .unwrap_or_else(|| "null".to_owned()),
                send_latency.percentile_upper_bound(0.50).unwrap_or(0),
                send_latency.percentile_upper_bound(0.99).unwrap_or(0),
                send_latency.percentile_upper_bound(0.999).unwrap_or(0),
                execution_percentile_json(0.50),
                execution_percentile_json(0.99),
                execution_percentile_json(0.999),
            );
            fs::write(&report_path, report)?;
            println!("JSON measurement summary: {report_path}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zipf_sampler_is_repeatable_for_a_seed() {
        let mut first = ZipfSampler::new(16, 1.5, 42).unwrap();
        let mut second = ZipfSampler::new(16, 1.5, 42).unwrap();
        let first_samples = (0..10_000).map(|_| first.next_expert()).collect::<Vec<_>>();
        let second_samples = (0..10_000)
            .map(|_| second.next_expert())
            .collect::<Vec<_>>();
        assert_eq!(first_samples, second_samples);
    }

    #[test]
    fn zipf_skew_increases_the_most_popular_expert_share() {
        let mut sampler = ZipfSampler::new(16, 1.5, 7).unwrap();
        let mut counts = [0u64; 16];
        for _ in 0..100_000 {
            counts[sampler.next_expert() as usize] += 1;
        }
        assert!(counts[0] > counts[1]);
        assert!(counts[1] > counts[2]);
        assert!(counts[0] > 40_000);
    }

    #[test]
    fn histogram_reports_ranked_upper_bounds() {
        let mut histogram = LatencyHistogram::new();
        for nanos in [100, 200, 300, 400, 500] {
            histogram.record(Duration::from_nanos(nanos));
        }
        assert!(histogram.percentile_upper_bound(0.50).unwrap() >= 300);
        assert!(histogram.percentile_upper_bound(0.99).unwrap() >= 500);
        assert!(histogram.percentile_upper_bound(0.999).unwrap() >= 500);
    }

    #[test]
    fn zipf_sampler_rejects_invalid_parameters() {
        assert!(ZipfSampler::new(0, 1.0, 1).is_err());
        assert!(ZipfSampler::new(8, f64::NAN, 1).is_err());
        assert!(ZipfSampler::new(8, -0.1, 1).is_err());
    }

    #[test]
    fn receiver_scrape_sums_drops_and_reads_primary_queue_counters() {
        let metrics = "\
moe_rx_packets_total 90
moe_saturated_drops_total 7
moe_invalid_packets_total 2
moe_closed_queue_drops_total 1
moe_fallback_routed_total 4
moe_expert_drops_total{expert_id=\"0\"} 5
moe_expert_queue_depth{expert_id=\"0\"} 12
moe_expert_queue_depth{expert_id=\"1\"} 20
";
        let snapshot = scrape_receiver_metrics(metrics);
        assert_eq!(snapshot.received_packets, 90);
        assert_eq!(snapshot.application_drops, 10);
        assert_eq!(snapshot.primary_expert_overflows, 5);
        assert_eq!(snapshot.fallback_routed, 4);
        assert_eq!(snapshot.max_expert_queue_depth, 20);
    }

    #[test]
    fn receiver_counter_deltas_preserve_current_queue_gauge() {
        let before = ReceiverMetrics {
            received_packets: 20,
            application_drops: 4,
            primary_expert_overflows: 3,
            fallback_routed: 1,
            max_expert_queue_depth: 12,
            ..ReceiverMetrics::default()
        };
        let after = ReceiverMetrics {
            received_packets: 50,
            application_drops: 14,
            primary_expert_overflows: 9,
            fallback_routed: 5,
            max_expert_queue_depth: 20,
            ..ReceiverMetrics::default()
        }
        .counter_delta(before);
        assert_eq!(after.received_packets, 30);
        assert_eq!(after.application_drops, 10);
        assert_eq!(after.primary_expert_overflows, 6);
        assert_eq!(after.fallback_routed, 4);
        assert_eq!(after.max_expert_queue_depth, 20);
    }

    #[test]
    fn execution_latency_percentile_uses_cumulative_histogram_buckets() {
        let receiver = ReceiverMetrics {
            execution_latency_buckets: vec![(100, 0), (151, 1), (1_515, 2), (10_000, 2)],
            ..ReceiverMetrics::default()
        };
        assert_eq!(receiver.execution_percentile_ns(0.50), Some(151));
        assert_eq!(receiver.execution_percentile_ns(0.99), Some(1_515));
        assert_eq!(receiver.execution_percentile_ns(0.999), Some(1_515));
    }
}
