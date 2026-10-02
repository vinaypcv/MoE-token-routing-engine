use std::fmt::Write as FmtWrite;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub const EXPERT_METRIC_COUNT: usize = 8;
pub const DROP_REASON_COUNT: usize = 10;
pub const LATENCY_PHASE_COUNT: usize = 4;
pub const LATENCY_PHASE_NAMES: [&str; LATENCY_PHASE_COUNT] =
    ["ingress", "queue_wait", "execution", "completion"];
pub const DROP_REASON_NAMES: [&str; DROP_REASON_COUNT] = [
    "truncated",
    "unsupported_ether_type",
    "unsupported_ip_protocol",
    "invalid_ipv4_header",
    "fragmented_ipv4",
    "invalid_udp_length",
    "invalid_magic",
    "unknown_expert",
    "queue_full",
    "queue_closed",
];
const LATENCY_BUCKETS_MICROS: [u64; 9] = [
    100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
];
const EXECUTION_LATENCY_LOWEST_NS: u64 = 100;
const EXECUTION_LATENCY_HIGHEST_NS: u64 = 10_000_000_000;
const EXECUTION_LATENCY_PRECISION: f64 = 1.01;

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
    pub job_latency_bucket: [AtomicU64; LATENCY_BUCKETS_MICROS.len()],
    pub job_latency_sum_nanos: AtomicU64,
    pub job_latency_count: AtomicU64,
    execution_latency_bounds_nanos: Vec<u64>,
    execution_latency_bucket: Vec<AtomicU64>,
    pub execution_latency_sum_nanos: AtomicU64,
    pub execution_latency_count: AtomicU64,
    pub execution_timestamp_errors_total: AtomicU64,
    pub phase_latency_bucket: [[AtomicU64; LATENCY_BUCKETS_MICROS.len()]; LATENCY_PHASE_COUNT],
    pub phase_latency_sum_nanos: [AtomicU64; LATENCY_PHASE_COUNT],
    pub phase_latency_count: [AtomicU64; LATENCY_PHASE_COUNT],
    pub service_capacity_per_tick: AtomicU64,
    pub drop_reasons: [AtomicU64; DROP_REASON_COUNT],
    pub predictions_total: AtomicU64,
    pub reroutes_total: AtomicU64,
    pub low_confidence_predictions_total: AtomicU64,
    pub fallback_routed_total: AtomicU64,
    pub nack_requests_total: AtomicU64,
    pub nack_tx_sent_total: AtomicU64,
    pub nack_tx_unavailable_total: AtomicU64,
    pub expert_dispatched: [AtomicU64; EXPERT_METRIC_COUNT],
    pub expert_drops: [AtomicU64; EXPERT_METRIC_COUNT],
    pub expert_queue_depth: [AtomicU64; EXPERT_METRIC_COUNT],
}

impl Default for PipelineTelemetry {
    fn default() -> Self {
        let mut execution_latency_bounds_nanos = vec![EXECUTION_LATENCY_LOWEST_NS];
        while *execution_latency_bounds_nanos
            .last()
            .expect("execution histogram begins with a bound")
            < EXECUTION_LATENCY_HIGHEST_NS
        {
            let previous = *execution_latency_bounds_nanos
                .last()
                .expect("execution histogram has a bound");
            let next = ((previous as f64 * EXECUTION_LATENCY_PRECISION).ceil() as u64)
                .max(previous + 1)
                .min(EXECUTION_LATENCY_HIGHEST_NS);
            execution_latency_bounds_nanos.push(next);
        }
        let execution_latency_bucket = execution_latency_bounds_nanos
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect();
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
            job_latency_bucket: std::array::from_fn(|_| AtomicU64::new(0)),
            job_latency_sum_nanos: AtomicU64::new(0),
            job_latency_count: AtomicU64::new(0),
            execution_latency_bounds_nanos,
            execution_latency_bucket,
            execution_latency_sum_nanos: AtomicU64::new(0),
            execution_latency_count: AtomicU64::new(0),
            execution_timestamp_errors_total: AtomicU64::new(0),
            phase_latency_bucket: std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0))
            }),
            phase_latency_sum_nanos: std::array::from_fn(|_| AtomicU64::new(0)),
            phase_latency_count: std::array::from_fn(|_| AtomicU64::new(0)),
            service_capacity_per_tick: AtomicU64::new(0),
            drop_reasons: std::array::from_fn(|_| AtomicU64::new(0)),
            predictions_total: AtomicU64::new(0),
            reroutes_total: AtomicU64::new(0),
            low_confidence_predictions_total: AtomicU64::new(0),
            fallback_routed_total: AtomicU64::new(0),
            nack_requests_total: AtomicU64::new(0),
            nack_tx_sent_total: AtomicU64::new(0),
            nack_tx_unavailable_total: AtomicU64::new(0),
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

    pub fn record_job_latency(&self, elapsed: std::time::Duration) {
        self.record_phase_latency(3, elapsed);
    }

    pub fn record_execution_latency_ns(&self, elapsed_nanos: u64) {
        let bucket_index = self
            .execution_latency_bounds_nanos
            .partition_point(|bound| *bound < elapsed_nanos)
            .min(self.execution_latency_bucket.len() - 1);
        for bucket in self.execution_latency_bucket.iter().skip(bucket_index) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
        self.execution_latency_sum_nanos
            .fetch_add(elapsed_nanos, Ordering::Relaxed);
        self.execution_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_phase_latency(&self, phase: usize, elapsed: std::time::Duration) {
        if phase >= LATENCY_PHASE_COUNT {
            return;
        }
        let elapsed_nanos = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        let elapsed_micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.phase_latency_sum_nanos[phase].fetch_add(elapsed_nanos, Ordering::Relaxed);
        self.phase_latency_count[phase].fetch_add(1, Ordering::Relaxed);
        for (index, boundary) in LATENCY_BUCKETS_MICROS.iter().enumerate() {
            if elapsed_micros <= *boundary {
                self.phase_latency_bucket[phase][index].fetch_add(1, Ordering::Relaxed);
            }
        }
        if phase == 3 {
            self.job_latency_sum_nanos
                .fetch_add(elapsed_nanos, Ordering::Relaxed);
            self.job_latency_count.fetch_add(1, Ordering::Relaxed);
            for (index, boundary) in LATENCY_BUCKETS_MICROS.iter().enumerate() {
                if elapsed_micros <= *boundary {
                    self.job_latency_bucket[index].fetch_add(1, Ordering::Relaxed);
                }
            }
        }
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
        let _ = writeln!(output, "# HELP moe_job_latency_seconds End-to-end job latency from queue admission to completion");
        let _ = writeln!(output, "# TYPE moe_job_latency_seconds histogram");
        for (index, boundary) in LATENCY_BUCKETS_MICROS.iter().enumerate() {
            let _ = writeln!(
                output,
                "moe_job_latency_seconds_bucket{{le=\"{}\"}} {}",
                *boundary as f64 / 1_000_000.0,
                self.job_latency_bucket[index].load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(
            output,
            "moe_job_latency_seconds_bucket{{le=\"+Inf\"}} {}",
            self.job_latency_count.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "moe_job_latency_seconds_sum {}",
            self.job_latency_sum_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0
        );
        let _ = writeln!(
            output,
            "moe_job_latency_seconds_count {}",
            self.job_latency_count.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "# HELP moe_ingress_to_completion_latency_seconds Sender CLOCK_MONOTONIC T0 to expert worker completion"
        );
        let _ = writeln!(
            output,
            "# TYPE moe_ingress_to_completion_latency_seconds histogram"
        );
        for (index, boundary) in self.execution_latency_bounds_nanos.iter().enumerate() {
            let _ = writeln!(
                output,
                "moe_ingress_to_completion_latency_seconds_bucket{{le=\"{}\"}} {}",
                *boundary as f64 / 1_000_000_000.0,
                self.execution_latency_bucket[index].load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(
            output,
            "moe_ingress_to_completion_latency_seconds_bucket{{le=\"+Inf\"}} {}",
            self.execution_latency_count.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "moe_ingress_to_completion_latency_seconds_sum {}",
            self.execution_latency_sum_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0
        );
        let _ = writeln!(
            output,
            "moe_ingress_to_completion_latency_seconds_count {}",
            self.execution_latency_count.load(Ordering::Relaxed)
        );
        render_counter(
            &mut output,
            "moe_execution_timestamp_errors_total",
            "Invalid, future, or unavailable cross-process monotonic timestamps",
            self.execution_timestamp_errors_total
                .load(Ordering::Relaxed),
        );
        for (phase, phase_name) in LATENCY_PHASE_NAMES.iter().enumerate() {
            let _ = writeln!(
                output,
                "# HELP moe_{phase_name}_latency_seconds {phase_name} latency histogram"
            );
            let _ = writeln!(output, "# TYPE moe_{phase_name}_latency_seconds histogram");
            for (index, boundary) in LATENCY_BUCKETS_MICROS.iter().enumerate() {
                let _ = writeln!(
                    output,
                    "moe_{phase_name}_latency_seconds_bucket{{le=\"{}\"}} {}",
                    *boundary as f64 / 1_000_000.0,
                    self.phase_latency_bucket[phase][index].load(Ordering::Relaxed)
                );
            }
            let _ = writeln!(
                output,
                "moe_{phase_name}_latency_seconds_bucket{{le=\"+Inf\"}} {}",
                self.phase_latency_count[phase].load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "moe_{phase_name}_latency_seconds_sum {}",
                self.phase_latency_sum_nanos[phase].load(Ordering::Relaxed) as f64
                    / 1_000_000_000.0
            );
            let _ = writeln!(
                output,
                "moe_{phase_name}_latency_seconds_count {}",
                self.phase_latency_count[phase].load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(output, "# HELP moe_service_capacity_per_tick Current synthetic service capacity per 100ms tick");
        let _ = writeln!(output, "# TYPE moe_service_capacity_per_tick gauge");
        let _ = writeln!(
            output,
            "moe_service_capacity_per_tick {}",
            self.service_capacity_per_tick.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "# HELP moe_drop_reason_total Dropped frames by parser or backpressure reason"
        );
        let _ = writeln!(output, "# TYPE moe_drop_reason_total counter");
        for (index, reason) in DROP_REASON_NAMES.iter().enumerate() {
            let _ = writeln!(
                output,
                "moe_drop_reason_total{{reason=\"{reason}\"}} {}",
                self.drop_reasons[index].load(Ordering::Relaxed)
            );
        }
        render_counter(
            &mut output,
            "moe_predictions_total",
            "Token-aware expert predictions attempted",
            self.predictions_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_reroutes_total",
            "Jobs routed to an expert different from the packet hint",
            self.reroutes_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_low_confidence_predictions_total",
            "Predictions rejected because confidence was below threshold",
            self.low_confidence_predictions_total
                .load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_fallback_routed_total",
            "Jobs rerouted to the configured fallback expert",
            self.fallback_routed_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_nack_requests_total",
            "Sequence-gap NACK requests detected in userspace",
            self.nack_requests_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_nack_tx_sent_total",
            "NACK frames submitted to the AF_XDP TX ring",
            self.nack_tx_sent_total.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "moe_nack_tx_unavailable_total",
            "NACK frames not sent because a TX frame or ring slot was unavailable",
            self.nack_tx_unavailable_total.load(Ordering::Relaxed),
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
                let dashboard_path = request.starts_with("GET / ");
                let (status, content_type, body) = if metrics_path {
                    (
                        "200 OK",
                        "text/plain; version=0.0.4; charset=utf-8",
                        telemetry.render_prometheus(),
                    )
                } else if dashboard_path {
                    (
                        "200 OK",
                        "text/html; charset=utf-8",
                        LIVE_DASHBOARD.to_owned(),
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

const LIVE_DASHBOARD: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MoE Pipeline Live Metrics</title>
<style>
:root{color-scheme:dark;--bg:#101719;--panel:#192326;--line:#344448;--text:#e7f0ed;--muted:#9db0ad;--green:#66d6a5;--amber:#f2bd65;--red:#f07878}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui,sans-serif}main{max-width:1100px;margin:0 auto;padding:28px 22px}header{display:flex;justify-content:space-between;align-items:end;border-bottom:1px solid var(--line);padding-bottom:18px;margin-bottom:22px}h1{font-size:24px;margin:0}p{margin:4px 0 0;color:var(--muted)}.status{font-size:13px;color:var(--green)}.grid{display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:12px}.metric,.panel{background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:16px}.label{font-size:12px;color:var(--muted);text-transform:uppercase}.value{font-size:26px;font-variant-numeric:tabular-nums;margin-top:5px}.panel{margin-top:14px}.panel h2{font-size:16px;margin:0 0 12px}.rows{display:grid;grid-template-columns:repeat(4,minmax(120px,1fr));gap:8px}.expert{border-top:1px solid var(--line);padding:9px 0}.expert strong{display:block;font-variant-numeric:tabular-nums}.bar{height:5px;background:#304044;margin-top:7px}.bar i{display:block;height:100%;background:var(--green)}footer{color:var(--muted);font-size:12px;margin-top:16px}@media(max-width:720px){.grid{grid-template-columns:repeat(2,1fr)}.rows{grid-template-columns:repeat(2,1fr)}header{align-items:start;gap:8px;flex-direction:column}}
</style>
</head>
<body><main>
<header><div><h1>MoE Pipeline</h1><p>Live process telemetry · refreshes every 2 seconds</p></div><div id="status" class="status">Connecting…</div></header>
<section class="grid">
<article class="metric"><div class="label">RX packets</div><div class="value" id="rx">—</div></article>
<article class="metric"><div class="label">Dispatched</div><div class="value" id="dispatch">—</div></article>
<article class="metric"><div class="label">Processed</div><div class="value" id="processed">—</div></article>
<article class="metric"><div class="label">Queue drops</div><div class="value" id="drops">—</div></article>
</section>
<section class="panel"><h2>Expert queues and drops</h2><div class="rows" id="experts"></div></section>
<section class="panel"><h2>Frame and error counters</h2><div class="grid">
<div><div class="label">Invalid packets</div><strong id="invalid">—</strong></div><div><div class="label">Execution errors</div><strong id="errors">—</strong></div><div><div class="label">Frames recycled</div><strong id="recycled">—</strong></div><div><div class="label">Recycle failures</div><strong id="recycle-errors">—</strong></div>
</div></section>
<footer>Demo mode counts received UDP traffic and simulates bounded worker service; it is not AF_XDP/NIC telemetry. The XDP loader reports real counters only while attached to a supported interface. Prometheus endpoint: <a href="/metrics" style="color:var(--green)">/metrics</a></footer>
</main>
<script>
const numberFmt=new Intl.NumberFormat();
function parseMetrics(text){const values=new Map();for(const line of text.split(/\r?\n/)){if(!line||line[0]==='#')continue;const split=line.lastIndexOf(' ');if(split<0)continue;values.set(line.slice(0,split),Number(line.slice(split+1)));}return values;}
async function update(){try{const response=await fetch('/metrics',{cache:'no-store'});if(!response.ok)throw new Error('HTTP '+response.status);const m=parseMetrics(await response.text());const get=(name)=>m.get(name)||0;for(const [id,name] of [['rx','moe_rx_packets_total'],['dispatch','moe_dispatched_total'],['processed','moe_processed_jobs_total'],['drops','moe_saturated_drops_total'],['invalid','moe_invalid_packets_total'],['errors','moe_execution_errors_total'],['recycled','moe_recycled_frames_total'],['recycle-errors','moe_recycle_failures_total']])document.getElementById(id).textContent=numberFmt.format(get(name));let html='';for(let i=0;i<8;i++){const depth=get(`moe_expert_queue_depth{expert_id="${i}"}`),drop=get(`moe_expert_drops_total{expert_id="${i}"}`),sent=get(`moe_expert_dispatched_total{expert_id="${i}"}`);html+=`<div class="expert"><span>Expert ${i}</span><strong>${numberFmt.format(depth)} queued</strong><small>${numberFmt.format(sent)} dispatched · ${numberFmt.format(drop)} dropped</small><div class="bar"><i style="width:${Math.min(100,depth/10)}%"></i></div></div>`;}document.getElementById('experts').innerHTML=html;document.getElementById('status').textContent='LIVE · '+new Date().toLocaleTimeString();}catch(error){document.getElementById('status').textContent='Metrics unavailable · '+error.message;}}update();setInterval(update,2000);
</script></body></html>"#;

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

    #[test]
    fn latency_histogram_reports_count_and_inclusive_buckets() {
        let metrics = PipelineTelemetry::default();
        metrics.record_job_latency(std::time::Duration::from_micros(50));
        metrics.record_job_latency(std::time::Duration::from_micros(700));
        let rendered = metrics.render_prometheus();

        assert!(rendered.contains("moe_job_latency_seconds_count 2"));
        assert!(rendered.contains("moe_job_latency_seconds_bucket{le=\"0.0001\"} 1"));
        assert!(rendered.contains("moe_job_latency_seconds_bucket{le=\"0.0005\"} 1"));
        assert!(rendered.contains("moe_job_latency_seconds_bucket{le=\"0.001\"} 2"));
        assert!(rendered.contains("moe_job_latency_seconds_bucket{le=\"+Inf\"} 2"));
    }

    #[test]
    fn execution_latency_histogram_reports_cumulative_nanosecond_buckets() {
        let metrics = PipelineTelemetry::default();
        metrics.record_execution_latency_ns(150);
        metrics.record_execution_latency_ns(1_500);
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("moe_ingress_to_completion_latency_seconds_count 2"));
        assert!(rendered
            .contains("moe_ingress_to_completion_latency_seconds_bucket{le=\"0.0000001\"} 0"));
        assert!(rendered
            .contains("moe_ingress_to_completion_latency_seconds_bucket{le=\"0.000000151\"} 1"));
        assert!(
            rendered.contains("moe_ingress_to_completion_latency_seconds_bucket{le=\"+Inf\"} 2")
        );
    }
}
