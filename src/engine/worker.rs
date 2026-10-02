use std::hint::black_box;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::Receiver;
use xsk_rs::umem::Umem;

use super::backpressure::{FrameGuard, FrameRecycler};
use super::clock::measurement_now_ns;
use super::dispatcher::{TokenJob, TOKEN_HEADER_LEN};
use super::telemetry::{PipelineTelemetry, EXPERT_METRIC_COUNT};

pub trait ExpertModel: Send + Sync + 'static {
    fn id(&self) -> u8;
    fn process_token(&self, token_id: u64, payload: &[u8]) -> Result<(), &'static str>;
}

pub struct LinearExpertModel {
    expert_id: u8,
    weights: Vec<f32>,
}

fn feature_payload(
    packet: &[u8],
    payload_offset: usize,
    feature_length: usize,
    has_ingress_timestamp: bool,
) -> &[u8] {
    let timestamp_feature_len = if has_ingress_timestamp { 8 } else { 0 };
    if feature_length < timestamp_feature_len {
        return &[];
    }
    let feature_start = payload_offset + TOKEN_HEADER_LEN + timestamp_feature_len;
    let feature_end = payload_offset + TOKEN_HEADER_LEN + feature_length;
    packet.get(feature_start..feature_end).unwrap_or(&[])
}

impl LinearExpertModel {
    pub fn new(expert_id: u8, feature_dim: usize) -> Self {
        let weights = (0..feature_dim)
            .map(|index| (index as f32) * 0.01)
            .collect();
        Self { expert_id, weights }
    }
}

impl ExpertModel for LinearExpertModel {
    fn id(&self) -> u8 {
        self.expert_id
    }

    fn process_token(&self, _token_id: u64, payload: &[u8]) -> Result<(), &'static str> {
        if payload.is_empty() {
            return Err("token contains no model feature payload");
        }
        if self.weights.is_empty() {
            return Err("expert model has no weights");
        }

        let mut accumulator = 0.0f32;
        for (index, byte) in payload.iter().enumerate() {
            accumulator += f32::from(*byte) * self.weights[index % self.weights.len()];
        }
        black_box(accumulator);
        Ok(())
    }
}

pub(crate) fn spawn_expert_worker<M: ExpertModel + ?Sized>(
    model: Arc<M>,
    receiver: Receiver<TokenJob>,
    umem: Umem,
    recycler: Arc<FrameRecycler>,
    telemetry: Arc<PipelineTelemetry>,
) -> std::io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name(format!("expert-{}", model.id()))
        .spawn(move || {
            while let Ok(job) = receiver.recv() {
                let _frame_guard = FrameGuard::new(job.frame, Arc::clone(&recycler));
                telemetry.record_phase_latency(1, job.enqueued_at.elapsed());
                let expert_id = usize::from(job.expert_id);
                if expert_id < EXPERT_METRIC_COUNT {
                    telemetry.expert_queue_depth[expert_id]
                        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                }

                let packet_data = unsafe { umem.data(&job.frame) };
                let features = feature_payload(
                    packet_data.contents(),
                    job.payload_offset,
                    job.feature_length,
                    job.ingress_timestamp_ns.is_some(),
                );
                let execution_started = std::time::Instant::now();
                let processing_result = model.process_token(job.token_id, features);
                let completion_timestamp_ns =
                    job.ingress_timestamp_ns.map(|_| measurement_now_ns());
                match processing_result {
                    Ok(()) => {
                        telemetry
                            .processed_jobs
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        telemetry.record_phase_latency(2, execution_started.elapsed());
                        telemetry.record_job_latency(job.received_at.elapsed());
                    }
                    Err(_) => {
                        telemetry
                            .execution_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        telemetry.record_phase_latency(2, execution_started.elapsed());
                        telemetry.record_job_latency(job.received_at.elapsed());
                    }
                }
                if let Some(ingress_timestamp_ns) = job.ingress_timestamp_ns {
                    match completion_timestamp_ns
                        .and_then(Result::ok)
                        .and_then(|completed_ns| completed_ns.checked_sub(ingress_timestamp_ns))
                    {
                        Some(elapsed_ns) if ingress_timestamp_ns > 0 => {
                            telemetry.record_execution_latency_ns(elapsed_ns);
                        }
                        _ => {
                            telemetry
                                .execution_timestamp_errors_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_slice_skips_full_v2_header_and_optional_timestamp_prefix() {
        let payload_offset = 7;
        let mut packet = vec![0u8; payload_offset + TOKEN_HEADER_LEN + 8 + 3];
        packet[payload_offset..payload_offset + TOKEN_HEADER_LEN].fill(0x11);
        packet[payload_offset + TOKEN_HEADER_LEN..payload_offset + TOKEN_HEADER_LEN + 8].fill(0x22);
        packet[payload_offset + TOKEN_HEADER_LEN + 8..].copy_from_slice(&[3, 4, 5]);

        assert_eq!(
            feature_payload(&packet, payload_offset, 11, true),
            &[3, 4, 5]
        );
        assert_eq!(
            feature_payload(&packet, payload_offset, 11, false),
            &[0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 3, 4, 5]
        );
    }
}
