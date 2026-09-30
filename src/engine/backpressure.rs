use std::hint::black_box;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use xsk_rs::umem::Umem;
use xsk_rs::FrameDesc;

use super::dispatcher::{EngineDispatcher, TokenJob};

pub const DEFAULT_EXPERT_QUEUE_CAPACITY: usize = 1024;
pub const DEFAULT_EXPERT_COUNT: usize = 8;

#[derive(Default)]
pub struct SystemMetrics {
    pub total_rx: AtomicU64,
    pub dispatched: AtomicU64,
    pub processed: AtomicU64,
    pub saturated_drops: AtomicU64,
    pub invalid_packets: AtomicU64,
    pub closed_queue_drops: AtomicU64,
    pub recycle_failures: AtomicU64,
}

struct FrameRecycler {
    sender: Sender<FrameDesc>,
    metrics: Arc<SystemMetrics>,
}

impl FrameRecycler {
    fn recycle(&self, frame: FrameDesc) {
        if self.sender.send(frame).is_err() {
            self.metrics
                .recycle_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct FrameGuard {
    frame: Option<FrameDesc>,
    recycler: Arc<FrameRecycler>,
}

impl FrameGuard {
    fn new(frame: FrameDesc, recycler: Arc<FrameRecycler>) -> Self {
        Self {
            frame: Some(frame),
            recycler,
        }
    }
}

impl Drop for FrameGuard {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.recycler.recycle(frame);
        }
    }
}

pub struct BackpressureRouter {
    dispatcher: Option<EngineDispatcher>,
    recycle_sender: Option<Sender<FrameDesc>>,
    recycle_receiver: Receiver<FrameDesc>,
    workers: Vec<JoinHandle<()>>,
    metrics: Arc<SystemMetrics>,
}

impl BackpressureRouter {
    pub fn new(
        umem: Umem,
        expert_count: usize,
        queue_capacity: usize,
        frame_pool_capacity: usize,
    ) -> io::Result<Self> {
        if expert_count == 0 || queue_capacity == 0 || frame_pool_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expert count, queue capacity, and frame pool capacity must be non-zero",
            ));
        }

        let metrics = Arc::new(SystemMetrics::default());
        let (recycle_sender, recycle_receiver) = bounded(frame_pool_capacity);
        let recycler = Arc::new(FrameRecycler {
            sender: recycle_sender.clone(),
            metrics: Arc::clone(&metrics),
        });
        let mut expert_senders = Vec::with_capacity(expert_count);
        let mut workers = Vec::with_capacity(expert_count);

        for expert_id in 0..expert_count {
            let (job_sender, job_receiver) = bounded::<TokenJob>(queue_capacity);
            expert_senders.push(job_sender);
            let worker_umem = umem.clone();
            let worker_recycler = Arc::clone(&recycler);
            let worker_metrics = Arc::clone(&metrics);
            let worker = thread::Builder::new()
                .name(format!("expert-{expert_id}"))
                .spawn(move || {
                    while let Ok(job) = job_receiver.recv() {
                        let _frame_guard = FrameGuard::new(job.frame, Arc::clone(&worker_recycler));
                        let payload_marker = unsafe {
                            worker_umem
                                .data(&job.frame)
                                .contents()
                                .get(job.payload_offset)
                                .copied()
                        };

                        if payload_marker == Some(0x77) {
                            black_box((job.token_id, job.expert_id));
                            worker_metrics.processed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })?;
            workers.push(worker);
        }

        Ok(Self {
            dispatcher: Some(EngineDispatcher::new(expert_senders, Arc::clone(&metrics))),
            recycle_sender: Some(recycle_sender),
            recycle_receiver,
            workers,
            metrics,
        })
    }

    pub fn dispatch_frame(
        &self,
        frame: FrameDesc,
        frame_bytes: &[u8],
    ) -> Result<(), (FrameDesc, super::dispatcher::FrameDropReason)> {
        self.dispatcher
            .as_ref()
            .expect("router dispatcher is active")
            .dispatch_frame(frame, frame_bytes)
            .map(|_| ())
    }

    pub fn metrics(&self) -> Arc<SystemMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn drain_recycled(&self, output: &mut Vec<FrameDesc>, limit: usize) -> usize {
        let mut drained = 0;
        while drained < limit {
            match self.recycle_receiver.try_recv() {
                Ok(frame) => {
                    output.push(frame);
                    drained += 1;
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        drained
    }

    pub fn shutdown(mut self) -> Vec<FrameDesc> {
        drop(self.dispatcher.take());
        drop(self.recycle_sender.take());
        let mut pending_workers = std::mem::take(&mut self.workers);
        let mut recycled_frames = Vec::new();

        while !pending_workers.is_empty() {
            recycled_frames.extend(self.recycle_receiver.try_iter());
            let mut unfinished_workers = Vec::with_capacity(pending_workers.len());
            for worker in pending_workers {
                if worker.is_finished() {
                    if worker.join().is_err() {
                        self.metrics
                            .recycle_failures
                            .fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    unfinished_workers.push(worker);
                }
            }
            pending_workers = unfinished_workers;
            if !pending_workers.is_empty() {
                thread::yield_now();
            }
        }

        recycled_frames.extend(self.recycle_receiver.try_iter());
        recycled_frames
    }
}
