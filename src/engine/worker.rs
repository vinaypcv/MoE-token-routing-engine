use std::hint::black_box;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::Receiver;
use xsk_rs::umem::Umem;

use super::backpressure::{FrameGuard, FrameRecycler};
use super::dispatcher::TokenJob;
use super::telemetry::{PipelineTelemetry, EXPERT_METRIC_COUNT};

pub trait ExpertModel: Send + Sync + 'static {
    fn id(&self) -> u8;
    fn process_token(&self, token_id: u64, payload: &[u8]) -> Result<(), &'static str>;
}

pub struct LinearExpertModel {
    expert_id: u8,
    weights: Vec<f32>,
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
                let expert_id = usize::from(job.expert_id);
                if expert_id < EXPERT_METRIC_COUNT {
                    telemetry.expert_queue_depth[expert_id]
                        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                }

                let packet_data = unsafe { umem.data(&job.frame) };
                let feature_start = job.payload_offset + 10;
                let feature_end = feature_start.saturating_add(job.feature_length);
                let features = packet_data
                    .contents()
                    .get(feature_start..feature_end)
                    .unwrap_or(&[]);
                match model.process_token(job.token_id, features) {
                    Ok(()) => {
                        telemetry
                            .processed_jobs
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(_) => {
                        telemetry
                            .execution_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        })
}
