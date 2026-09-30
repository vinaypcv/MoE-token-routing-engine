use moe_holistic_engine::engine::telemetry::{
    PipelineTelemetry, TelemetryServer, EXPERT_METRIC_COUNT,
};
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
    let mut service_interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            received = socket.recv_from(&mut packet) => {
                let (length, _) = received?;
                telemetry.rx_packets_total.fetch_add(1, Ordering::Relaxed);

                if length < 10 || packet[0] != 0x77 {
                    telemetry.invalid_packets_total.fetch_add(1, Ordering::Relaxed);
                    telemetry.record_recycled(1);
                    continue;
                }

                let expert_id = packet[9] as usize;
                if expert_id >= EXPERT_METRIC_COUNT {
                    telemetry.invalid_packets_total.fetch_add(1, Ordering::Relaxed);
                    telemetry.record_recycled(1);
                    continue;
                }

                let depth = &telemetry.expert_queue_depth[expert_id];
                let admitted = depth.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (current < queue_capacity).then_some(current + 1)
                });
                if admitted.is_ok() {
                    telemetry.dispatched_total.fetch_add(1, Ordering::Relaxed);
                    telemetry.expert_dispatched[expert_id].fetch_add(1, Ordering::Relaxed);
                    if length == 10 {
                        telemetry.execution_errors.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    telemetry.saturated_drops_total.fetch_add(1, Ordering::Relaxed);
                    telemetry.expert_drops[expert_id].fetch_add(1, Ordering::Relaxed);
                    telemetry.record_recycled(1);
                }
            }
            _ = service_interval.tick() => {
                for expert_id in 0..EXPERT_METRIC_COUNT {
                    let depth = &telemetry.expert_queue_depth[expert_id];
                    let mut current = depth.load(Ordering::Relaxed);
                    loop {
                        let completed = current.min(service_per_tick);
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
