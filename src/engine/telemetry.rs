use std::fmt::Write as FmtWrite;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub const EXPERT_METRIC_COUNT: usize = 8;

pub struct PipelineTelemetry {
    pub rx_packets_total: AtomicU64,
    pub dispatched_total: AtomicU64,
    pub processed_jobs: AtomicU64,
    pub execution_errors: AtomicU64,
    pub saturated_drops_total: AtomicU64,
    pub invalid_packets_total: AtomicU64,
    pub closed_queue_drops_total: AtomicU64,
    pub recycled_frames_total: AtomicU64,
    pub recycle_failures_total: AtomicU64,
    pub expert_dispatched: [AtomicU64; EXPERT_METRIC_COUNT],
    pub expert_drops: [AtomicU64; EXPERT_METRIC_COUNT],
    pub expert_queue_depth: [AtomicU64; EXPERT_METRIC_COUNT],
}

impl Default for PipelineTelemetry {
    fn default() -> Self {
        Self {
            rx_packets_total: AtomicU64::new(0),
            dispatched_total: AtomicU64::new(0),
            processed_jobs: AtomicU64::new(0),
            execution_errors: AtomicU64::new(0),
            saturated_drops_total: AtomicU64::new(0),
            invalid_packets_total: AtomicU64::new(0),
            closed_queue_drops_total: AtomicU64::new(0),
            recycled_frames_total: AtomicU64::new(0),
            recycle_failures_total: AtomicU64::new(0),
            expert_dispatched: std::array::from_fn(|_| AtomicU64::new(0)),
            expert_drops: std::array::from_fn(|_| AtomicU64::new(0)),
            expert_queue_depth: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl PipelineTelemetry {
    pub fn record_recycled(&self, count: u64) {
        self.recycled_frames_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        let mut output = String::with_capacity(4096);
        render_counter(
            &mut output,
            "moe_rx_packets_total",
            "Packets received from the AF_XDP RX ring",
            self.rx_packets_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_dispatched_total",
            "Token jobs accepted by expert queues",
            self.dispatched_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_processed_jobs_total",
            "Token jobs processed by expert models",
            self.processed_jobs.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_execution_errors_total",
            "Expert model execution errors",
            self.execution_errors.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_saturated_drops_total",
            "Jobs dropped because expert queues were full",
            self.saturated_drops_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_invalid_packets_total",
            "Packets rejected by the token parser or expert validation",
            self.invalid_packets_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_closed_queue_drops_total",
            "Jobs dropped because an expert queue was closed",
            self.closed_queue_drops_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_recycled_frames_total",
            "UMEM frames returned to the userspace free list",
            self.recycled_frames_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_recycle_failures_total",
            "UMEM frame recycle channel failures",
            self.recycle_failures_total.load(Ordering::Relaxed),
        );
        render_labeled_gauge_header(
            &mut output,
            "moe_expert_queue_depth",
            "Current queued jobs by expert",
        );
        render_labeled_counter_header(
            &mut output,
            "moe_expert_dispatched_total",
            "Jobs accepted by each expert queue",
        );
        render_labeled_counter_header(
            &mut output,
            "moe_expert_drops_total",
            "Jobs dropped by each expert queue",
        );
        for expert_id in 0..EXPERT_METRIC_COUNT {
            let _ = writeln!(
                output,
                "moe_expert_queue_depth{{expert_id=\"{expert_id}\"}} {}",
                self.expert_queue_depth[expert_id].load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "moe_expert_dispatched_total{{expert_id=\"{expert_id}\"}} {}",
                self.expert_dispatched[expert_id].load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "moe_expert_drops_total{{expert_id=\"{expert_id}\"}} {}",
                self.expert_drops[expert_id].load(Ordering::Relaxed)
            );
        }
        output
    }
}

fn render_counter(output: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} counter");
    let _ = writeln!(output, "{name} {value}");
}

fn render_labeled_gauge_header(output: &mut String, name: &str, help: &str) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} gauge");
}

fn render_labeled_counter_header(output: &mut String, name: &str, help: &str) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} counter");
}

pub struct TelemetryServer;

impl TelemetryServer {
    pub async fn run(addr: SocketAddr, telemetry: Arc<PipelineTelemetry>) -> io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        eprintln!("Prometheus metrics available at http://{addr}/metrics");

        loop {
            let (mut stream, _) = listener.accept().await?;
            let telemetry = Arc::clone(&telemetry);
            tokio::spawn(async move {
                let mut request = [0u8; 1024];
                let length = match stream.read(&mut request).await {
                    Ok(length) => length,
                    Err(_) => return,
                };
                let request = String::from_utf8_lossy(&request[..length]);
                let metrics_path = request.starts_with("GET /metrics ");
                let (status, content_type, body) = if metrics_path {
                    (
                        "200 OK",
                        "text/plain; version=0.0.4; charset=utf-8",
                        telemetry.render_prometheus(),
                    )
                } else {
                    (
                        "404 Not Found",
                        "text/plain; charset=utf-8",
                        "not found\n".to_owned(),
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_pipeline_and_expert_metrics() {
        let metrics = PipelineTelemetry::default();
        metrics.rx_packets_total.store(12, Ordering::Relaxed);
        metrics.expert_queue_depth[3].store(4, Ordering::Relaxed);
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("moe_rx_packets_total 12"));
        assert!(rendered.contains("moe_expert_queue_depth{expert_id=\"3\"} 4"));
    }
}
