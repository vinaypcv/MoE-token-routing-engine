use super::nack::{NackFrame, SequenceTracker};

#[derive(Clone, Copy)]
pub struct NackEndpoint {
    pub source_mac: [u8; 6],
    pub destination_mac: [u8; 6],
    pub source_ip: [u8; 4],
    pub destination_ip: [u8; 4],
    pub source_port: u16,
    pub destination_port: u16,
}

/// Turns sequence gaps observed by the receive loop into configured NACK frames.
#[derive(Default)]
pub struct ActiveNackLoop {
    sequence_tracker: SequenceTracker,
}

impl ActiveNackLoop {
    pub fn process_packet(
        &self,
        expert_id: u8,
        sequence_id: u32,
        endpoint: Option<NackEndpoint>,
    ) -> Option<NackFrame> {
        let request = self.sequence_tracker.observe(expert_id, sequence_id)?;
        let endpoint = endpoint?;
        Some(NackFrame::build(
            endpoint.source_mac,
            endpoint.destination_mac,
            endpoint.source_ip,
            endpoint.destination_ip,
            endpoint.source_port,
            endpoint.destination_port,
            request,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint() -> NackEndpoint {
        NackEndpoint {
            source_mac: [1, 2, 3, 4, 5, 6],
            destination_mac: [6, 5, 4, 3, 2, 1],
            source_ip: [10, 0, 0, 1],
            destination_ip: [10, 0, 0, 2],
            source_port: 9000,
            destination_port: 9001,
        }
    }

    #[test]
    fn formats_a_nack_when_a_configured_sequence_gap_is_seen() {
        let nack_loop = ActiveNackLoop::default();
        assert!(nack_loop.process_packet(2, 10, Some(endpoint())).is_none());
        assert!(nack_loop.process_packet(2, 12, Some(endpoint())).is_some());
    }

    #[test]
    fn tracks_gaps_without_transmitting_when_disabled() {
        let nack_loop = ActiveNackLoop::default();
        assert!(nack_loop.process_packet(2, 10, None).is_none());
        assert!(nack_loop.process_packet(2, 12, None).is_none());
        assert!(nack_loop.process_packet(2, 13, Some(endpoint())).is_none());
    }
}
