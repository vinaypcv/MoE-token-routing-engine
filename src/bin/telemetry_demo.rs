use moe_holistic_engine::engine::telemetry::{
    PipelineTelemetry, TelemetryServer, EXPERT_METRIC_COUNT,
};
use std::collections::VecDeque;
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let metrics_address: SocketAddr = env::var("MOE_METRICS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9100".to_owned())
        .parse()?;
    let traffic_address: SocketAddr = env::var("MOE_DEMO_UDP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9000".to_owned())
        .parse()?;
    let queue_capacity = env::var("MOE_DEMO_QUEUE_CAPACITY")
        .unwrap_or_else(|_| "256".to_owned())
        .parse::<u64>()?;
    let service_per_tick = env::var("MOE_DEMO_SERVICE_PER_TICK")
        .unwrap_or_else(|_| "24".to_owned())
        .parse::<u64>()?;
    let adaptive_backpressure = env::var("MOE_DEMO_ADAPTIVE_BACKPRESSURE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false);
    let load_shedding = env::var("MOE_DEMO_LOAD_SHEDDING")
        .unwrap_or_else(|_| "oldest".to_owned());
    if !matches!(load_shedding.as_str(), "oldest" | "priority" | "expert-aware") {
        return Err("MOE_DEMO_LOAD_SHEDDING must be oldest, priority, or expert-aware".into());
    }
    if queue_capacity == 0 || service_per_tick == 0 {
        return Err("demo queue capacity and service rate must be positive".into());
    }

    let telemetry = Arc::new(PipelineTelemetry::default());
    let producer_metrics = Arc::clone(&telemetry);
    let telemetry_address_for_server = metrics_address;
    tokio::spawn(async move {
        if let Err(error) =
            TelemetryServer::run(telemetry_address_for_server, producer_metrics).await
        {
            eprintln!("Demo telemetry HTTP server stopped: {error}");
        }
    });

    let socket = UdpSocket::bind(traffic_address).await?;
    eprintln!(
        "Traffic-driven telemetry preview: UDP {traffic_address}, dashboard http://{metrics_address}/"
    );
    eprintln!(
        "Synthetic worker model: queue capacity {queue_capacity}/expert; service {service_per_tick}/expert per 100ms"
    );

    let mut packet = vec![0u8; 65_535];
    let base_service_per_tick = service_per_tick;
    let mut current_service_per_tick = service_per_tick;
    telemetry
        .service_capacity_per_tick
        .store(current_service_per_tick, Ordering::Relaxed);
    let mut service_interval = tokio::time::interval(Duration::from_millis(100));
    let mut admitted_at: [VecDeque<(std::time::Instant, std::time::Instant)>; EXPERT_METRIC_COUNT] =
        std::array::from_fn(|_| VecDeque::new());
    loop {
        tokio::select! {
            received = socket.recv_from(&mut packet) => {
                let received_at = std::time::Instant::now();
                let (length, _) = received?;
                telemetry.rx_packets_total.fetch_add(1, Ordering::Relaxed);

                if length < 10 || packet[0] != 0x77 {
                    telemetry.invalid_packets_total.fetch_add(1, Ordering::Relaxed);
                    telemetry.drop_reasons[6].fetch_add(1, Ordering::Relaxed);
                    telemetry.record_recycled(1);
                    continue;
                }

                let expert_id = packet[9] as usize;
                if expert_id >= EXPERT_METRIC_COUNT {
                    telemetry.invalid_packets_total.fetch_add(1, Ordering::Relaxed);
                    telemetry.drop_reasons[7].fetch_add(1, Ordering::Relaxed);
                    telemetry.record_recycled(1);
                    continue;
                }

                let depth = &telemetry.expert_queue_depth[expert_id];
                let admission_limit = match load_shedding.as_str() {
                    "priority" if expert_id != 0 => queue_capacity.saturating_mul(3) / 4,
                    "expert-aware" => queue_capacity.saturating_sub(
                        telemetry.expert_drops[expert_id].load(Ordering::Relaxed) / 100,
                    ).max(queue_capacity / 2),
                    _ => queue_capacity,
                };
                let admitted = depth.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (current < admission_limit).then_some(current + 1)
                });
                if admitted.is_ok() {
                    let admitted_at_now = std::time::Instant::now();
                    telemetry.record_phase_latency(0, received_at.elapsed());
                    telemetry.dispatched_total.fetch_add(1, Ordering::Relaxed);
                    telemetry.expert_dispatched[expert_id].fetch_add(1, Ordering::Relaxed);
                    admitted_at[expert_id].push_back((received_at, admitted_at_now));
                    if length == 10 {
                        telemetry.execution_errors.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    telemetry.saturated_drops_total.fetch_add(1, Ordering::Relaxed);
                    telemetry.drop_reasons[8].fetch_add(1, Ordering::Relaxed);
                    telemetry.expert_drops[expert_id].fetch_add(1, Ordering::Relaxed);
                    telemetry.record_recycled(1);
                }
            }
            _ = service_interval.tick() => {
                if adaptive_backpressure {
                    let total_depth: u64 = telemetry
                        .expert_queue_depth
                        .iter()
                        .map(|depth| depth.load(Ordering::Relaxed))
                        .sum();
                    let queue_limit = queue_capacity.saturating_mul(EXPERT_METRIC_COUNT as u64);
                    if total_depth.saturating_mul(4) >= queue_limit.saturating_mul(3) {
                        current_service_per_tick = current_service_per_tick
                            .saturating_add(4)
                            .min(queue_capacity);
                    } else if total_depth.saturating_mul(4) <= queue_limit {
                        current_service_per_tick = current_service_per_tick
                            .saturating_sub(1)
                            .max(base_service_per_tick);
                    }
                    telemetry
                        .service_capacity_per_tick
                        .store(current_service_per_tick, Ordering::Relaxed);
                }
                for expert_id in 0..EXPERT_METRIC_COUNT {
                    let depth = &telemetry.expert_queue_depth[expert_id];
                    let mut current = depth.load(Ordering::Relaxed);
                    loop {
                        let completed = current.min(current_service_per_tick);
                        match depth.compare_exchange_weak(
                            current,
                            current - completed,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                if completed > 0 {
                                    telemetry.processed_jobs.fetch_add(completed, Ordering::Relaxed);
                                    telemetry.record_recycled(completed);
                                    for _ in 0..completed {
                                        if let Some((received_at, admitted_at)) =
                                            admitted_at[expert_id].pop_front()
                                        {
                                            telemetry.record_phase_latency(1, admitted_at.elapsed());
                                            telemetry.record_phase_latency(2, Duration::ZERO);
                                            telemetry.record_job_latency(received_at.elapsed());
                                        }
                                    }
                                }
                                break;
                            }
                            Err(observed) => current = observed,
                        }
                    }
                }
            }
        }
    }
}
