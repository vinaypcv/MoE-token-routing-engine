use std::sync::atomic::{AtomicU32, Ordering};

pub const EXPERT_TRACKER_COUNT: usize = 8;
pub const NACK_FRAME_CAPACITY: usize = 64;
const ETH_HEADER_LEN: usize = 14;
const IPV4_HEADER_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;
const NACK_PAYLOAD_LEN: usize = 10;
const NACK_MAGIC: u8 = 0x78;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackRequest {
    pub expert_id: u8,
    pub expected_sequence: u32,
    pub received_sequence: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackFrame {
    pub bytes: [u8; NACK_FRAME_CAPACITY],
    pub length: usize,
}

impl NackFrame {
    pub fn build(
        source_mac: [u8; 6],
        destination_mac: [u8; 6],
        source_ip: [u8; 4],
        destination_ip: [u8; 4],
        source_port: u16,
        destination_port: u16,
        request: NackRequest,
    ) -> Self {
        let mut bytes = [0u8; NACK_FRAME_CAPACITY];
        bytes[0..6].copy_from_slice(&destination_mac);
        bytes[6..12].copy_from_slice(&source_mac);
        bytes[12..14].copy_from_slice(&0x0800u16.to_be_bytes());

        let ip = ETH_HEADER_LEN;
        bytes[ip] = 0x45;
        let ip_total_length = (IPV4_HEADER_LEN + UDP_HEADER_LEN + NACK_PAYLOAD_LEN) as u16;
        bytes[ip + 2..ip + 4].copy_from_slice(&ip_total_length.to_be_bytes());
        bytes[ip + 8] = 64;
        bytes[ip + 9] = 17;
        bytes[ip + 12..ip + 16].copy_from_slice(&source_ip);
        bytes[ip + 16..ip + 20].copy_from_slice(&destination_ip);

        let udp = ip + IPV4_HEADER_LEN;
        bytes[udp..udp + 2].copy_from_slice(&source_port.to_be_bytes());
        bytes[udp + 2..udp + 4].copy_from_slice(&destination_port.to_be_bytes());
        bytes[udp + 4..udp + 6]
            .copy_from_slice(&((UDP_HEADER_LEN + NACK_PAYLOAD_LEN) as u16).to_be_bytes());

        let payload = udp + UDP_HEADER_LEN;
        bytes[payload] = NACK_MAGIC;
        bytes[payload + 1] = request.expert_id;
        bytes[payload + 2..payload + 6].copy_from_slice(&request.expected_sequence.to_be_bytes());
        bytes[payload + 6..payload + 10].copy_from_slice(&request.received_sequence.to_be_bytes());

        Self {
            bytes,
            length: payload + NACK_PAYLOAD_LEN,
        }
    }
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

    #[test]
    fn formats_fixed_size_nack_frame_without_heap_allocation() {
        let frame = NackFrame::build(
            [1, 2, 3, 4, 5, 6],
            [6, 5, 4, 3, 2, 1],
            [10, 0, 0, 1],
            [10, 0, 0, 2],
            9000,
            9001,
            NackRequest {
                expert_id: 3,
                expected_sequence: 12,
                received_sequence: 14,
            },
        );
        assert_eq!(frame.length, 52);
        assert_eq!(&frame.bytes[0..6], &[6, 5, 4, 3, 2, 1]);
        assert_eq!(frame.bytes[42], NACK_MAGIC);
        assert_eq!(&frame.bytes[44..48], &12u32.to_be_bytes());
        assert_eq!(&frame.bytes[48..52], &14u32.to_be_bytes());
    }
}
