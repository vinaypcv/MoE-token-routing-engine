use std::sync::atomic::{AtomicU32, Ordering};

pub const EXPERT_TRACKER_COUNT: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackRequest {
    pub expert_id: u8,
    pub expected_sequence: u32,
    pub received_sequence: u32,
}

pub struct SequenceTracker {
    next_sequence: [AtomicU32; EXPERT_TRACKER_COUNT],
}

impl Default for SequenceTracker {
    fn default() -> Self {
        Self {
            next_sequence: std::array::from_fn(|_| AtomicU32::new(0)),
        }
    }
}

impl SequenceTracker {
    pub fn observe(&self, expert_id: u8, sequence: u32) -> Option<NackRequest> {
        let index = usize::from(expert_id);
        let expected = self.next_sequence.get(index)?;
        let previous = expected.swap(sequence.wrapping_add(1), Ordering::Relaxed);
        if previous != 0 && sequence.wrapping_sub(previous) < u32::MAX / 2 && sequence > previous {
            Some(NackRequest {
                expert_id,
                expected_sequence: previous,
                received_sequence: sequence,
            })
        } else {
            None
        }
    }

    pub fn reset(&self) {
        for sequence in &self.next_sequence {
            sequence.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_gaps_without_allocating_per_packet_state() {
        let tracker = SequenceTracker::default();
        assert_eq!(tracker.observe(2, 10), None);
        assert_eq!(tracker.observe(2, 11), None);
        assert_eq!(
            tracker.observe(2, 14),
            Some(NackRequest {
                expert_id: 2,
                expected_sequence: 12,
                received_sequence: 14,
            })
        );
    }

    #[test]
    fn ignores_unknown_experts_and_can_reset() {
        let tracker = SequenceTracker::default();
        assert_eq!(tracker.observe(8, 1), None);
        tracker.observe(1, 10);
        tracker.reset();
        assert_eq!(tracker.observe(1, 20), None);
    }
}
