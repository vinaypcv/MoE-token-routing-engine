use std::io::{self, Write};

use xsk_rs::socket::TxQueue;
use xsk_rs::umem::{CompQueue, Umem};
use xsk_rs::FrameDesc;

use super::nack::NackFrame;

/// Owns TX-ring frame descriptors until the kernel returns them through the
/// completion ring. Callers must provide descriptors from the same UMEM.
pub struct NackTxQueue {
    umem: Umem,
    tx_queue: TxQueue,
    completion_queue: CompQueue,
    free_frames: Vec<FrameDesc>,
    completions: Vec<FrameDesc>,
}

impl NackTxQueue {
    pub fn new(
        umem: Umem,
        tx_queue: TxQueue,
        completion_queue: CompQueue,
        free_frames: Vec<FrameDesc>,
    ) -> Self {
        Self {
            umem,
            tx_queue,
            completion_queue,
            free_frames,
            completions: vec![FrameDesc::default(); 64],
        }
    }

    pub fn reclaim_completions(&mut self) -> usize {
        // SAFETY: completion descriptors belong to this UMEM and are not
        // accessed by userspace after the kernel releases them.
        let count = unsafe { self.completion_queue.consume(&mut self.completions) };
        self.free_frames
            .extend(self.completions.iter().take(count).copied());
        count
    }

    pub fn send(&mut self, frame: &NackFrame) -> io::Result<bool> {
        self.reclaim_completions();
        let Some(mut descriptor) = self.free_frames.pop() else {
            return Ok(false);
        };
        {
            // SAFETY: the descriptor came from this UMEM's free-frame pool
            // and has not been submitted to any ring.
            let mut data = unsafe { self.umem.data_mut(&mut descriptor) };
            let mut cursor = data.cursor();
            cursor.write_all(&frame.bytes[..frame.length])?;
        }

        // SAFETY: descriptor and queue are paired with this UMEM, and the
        // frame remains owned by the kernel until completion is reclaimed.
        let submitted = unsafe { self.tx_queue.produce_one_and_wakeup(&descriptor)? };
        if submitted == 0 {
            self.free_frames.push(descriptor);
            return Ok(false);
        }
        Ok(true)
    }

    pub fn into_parts(self) -> (TxQueue, CompQueue, Vec<FrameDesc>) {
        (self.tx_queue, self.completion_queue, self.free_frames)
    }
}
