use std::sync::atomic::Ordering;

use crossbeam_channel::{Sender, TrySendError};
use xsk_rs::FrameDesc;

use super::telemetry::{PipelineTelemetry, EXPERT_METRIC_COUNT};

const ETH_HEADER_LEN: usize = 14;
const IPV4_MIN_HEADER_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;
const TOKEN_HEADER_LEN: usize = 10;
const IPV4_ETHERTYPE: u16 = 0x0800;
const UDP_PROTOCOL: u8 = 17;
const TOKEN_MAGIC: u8 = 0x77;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDropReason {
    Truncated,
    UnsupportedEtherType,
    UnsupportedIpProtocol,
    InvalidIpv4Header,
    FragmentedIpv4,
    InvalidUdpLength,
    InvalidMagic,
    UnknownExpert,
    QueueFull,
    QueueClosed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedToken {
    pub token_id: u64,
    pub expert_id: u8,
    pub payload_offset: usize,
    pub feature_length: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct TokenJob {
    pub token_id: u64,
    pub expert_id: u8,
    pub frame: FrameDesc,
    pub payload_offset: usize,
    pub feature_length: usize,
}

pub struct EngineDispatcher {
    expert_senders: Vec<Sender<TokenJob>>,
    metrics: std::sync::Arc<PipelineTelemetry>,
}

impl EngineDispatcher {
    pub fn new(
        expert_senders: Vec<Sender<TokenJob>>,
        metrics: std::sync::Arc<PipelineTelemetry>,
    ) -> Self {
        Self {
            expert_senders,
            metrics,
        }
    }

    pub fn parse_frame(frame: &[u8]) -> Result<ParsedToken, FrameDropReason> {
        if frame.len() < ETH_HEADER_LEN {
            return Err(FrameDropReason::Truncated);
        }
        if u16::from_be_bytes([frame[12], frame[13]]) != IPV4_ETHERTYPE {
            return Err(FrameDropReason::UnsupportedEtherType);
        }

        let ip_offset = ETH_HEADER_LEN;
        if frame.len() < ip_offset + IPV4_MIN_HEADER_LEN {
            return Err(FrameDropReason::Truncated);
        }

        let version_ihl = frame[ip_offset];
        let version = version_ihl >> 4;
        let ihl_bytes = usize::from(version_ihl & 0x0f) * 4;
        if version != 4 || ihl_bytes < IPV4_MIN_HEADER_LEN {
            return Err(FrameDropReason::InvalidIpv4Header);
        }
        let ip_header_end = ip_offset
            .checked_add(ihl_bytes)
            .ok_or(FrameDropReason::Truncated)?;
        if frame.len() < ip_header_end {
            return Err(FrameDropReason::Truncated);
        }
        if frame[ip_offset + 9] != UDP_PROTOCOL {
            return Err(FrameDropReason::UnsupportedIpProtocol);
        }

        let fragment_flags = u16::from_be_bytes([frame[ip_offset + 6], frame[ip_offset + 7]]);
        if fragment_flags & 0x3fff != 0 {
            return Err(FrameDropReason::FragmentedIpv4);
        }

        let ip_total_len = usize::from(u16::from_be_bytes([
            frame[ip_offset + 2],
            frame[ip_offset + 3],
        ]));
        if ip_total_len < ihl_bytes + UDP_HEADER_LEN + TOKEN_HEADER_LEN {
            return Err(FrameDropReason::InvalidUdpLength);
        }
        let ip_packet_end = ip_offset
            .checked_add(ip_total_len)
            .ok_or(FrameDropReason::Truncated)?;
        if frame.len() < ip_packet_end {
            return Err(FrameDropReason::Truncated);
        }

        let udp_offset = ip_header_end;
        let udp_end = udp_offset
            .checked_add(UDP_HEADER_LEN)
            .ok_or(FrameDropReason::Truncated)?;
        if frame.len() < udp_end {
            return Err(FrameDropReason::Truncated);
        }
        let udp_total_len = usize::from(u16::from_be_bytes([
            frame[udp_offset + 4],
            frame[udp_offset + 5],
        ]));
        if udp_total_len < UDP_HEADER_LEN + TOKEN_HEADER_LEN
            || udp_total_len > ip_total_len - ihl_bytes
        {
            return Err(FrameDropReason::InvalidUdpLength);
        }

        let payload_offset = udp_end;
        let token_end = payload_offset
            .checked_add(TOKEN_HEADER_LEN)
            .ok_or(FrameDropReason::Truncated)?;
        let udp_packet_end = udp_offset
            .checked_add(udp_total_len)
            .ok_or(FrameDropReason::Truncated)?;
        if token_end > udp_packet_end || token_end > frame.len() {
            return Err(FrameDropReason::Truncated);
        }
        if frame[payload_offset] != TOKEN_MAGIC {
            return Err(FrameDropReason::InvalidMagic);
        }

        let token_id = u64::from_be_bytes(
            frame[payload_offset + 1..payload_offset + 9]
                .try_into()
                .map_err(|_| FrameDropReason::Truncated)?,
        );
        let expert_id = frame[payload_offset + 9];
        let feature_length = udp_total_len - UDP_HEADER_LEN - TOKEN_HEADER_LEN;

        Ok(ParsedToken {
            token_id,
            expert_id,
            payload_offset,
            feature_length,
        })
    }

    pub fn dispatch_frame(
        &self,
        frame_desc: FrameDesc,
        frame_bytes: &[u8],
    ) -> Result<ParsedToken, (FrameDesc, FrameDropReason)> {
        self.metrics
            .rx_packets_total
            .fetch_add(1, Ordering::Relaxed);
        let parsed = match Self::parse_frame(frame_bytes) {
            Ok(parsed) => parsed,
            Err(reason) => {
                self.metrics
                    .invalid_packets_total
                    .fetch_add(1, Ordering::Relaxed);
                return Err((frame_desc, reason));
            }
        };

        let Some(sender) = self.expert_senders.get(parsed.expert_id as usize) else {
            self.metrics
                .invalid_packets_total
                .fetch_add(1, Ordering::Relaxed);
            return Err((frame_desc, FrameDropReason::UnknownExpert));
        };

        let job = TokenJob {
            token_id: parsed.token_id,
            expert_id: parsed.expert_id,
            frame: frame_desc,
            payload_offset: parsed.payload_offset,
            feature_length: parsed.feature_length,
        };
        let expert_id = usize::from(parsed.expert_id);
        if expert_id < EXPERT_METRIC_COUNT {
            self.metrics.expert_queue_depth[expert_id].fetch_add(1, Ordering::Relaxed);
        }
        match sender.try_send(job) {
            Ok(()) => {
                self.metrics
                    .dispatched_total
                    .fetch_add(1, Ordering::Relaxed);
                if expert_id < EXPERT_METRIC_COUNT {
                    self.metrics.expert_dispatched[expert_id].fetch_add(1, Ordering::Relaxed);
                }
                Ok(parsed)
            }
            Err(TrySendError::Full(job)) => {
                if expert_id < EXPERT_METRIC_COUNT {
                    self.metrics.expert_queue_depth[expert_id].fetch_sub(1, Ordering::Relaxed);
                }
                self.metrics
                    .saturated_drops_total
                    .fetch_add(1, Ordering::Relaxed);
                if expert_id < EXPERT_METRIC_COUNT {
                    self.metrics.expert_drops[expert_id].fetch_add(1, Ordering::Relaxed);
                }
                Err((job.frame, FrameDropReason::QueueFull))
            }
            Err(TrySendError::Disconnected(job)) => {
                if expert_id < EXPERT_METRIC_COUNT {
                    self.metrics.expert_queue_depth[expert_id].fetch_sub(1, Ordering::Relaxed);
                }
                self.metrics
                    .closed_queue_drops_total
                    .fetch_add(1, Ordering::Relaxed);
                Err((job.frame, FrameDropReason::QueueClosed))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::telemetry::PipelineTelemetry;
    use crossbeam_channel::bounded;

    fn token_frame(expert: u8) -> Vec<u8> {
        let mut frame =
            vec![0u8; ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN + TOKEN_HEADER_LEN];
        frame[12..14].copy_from_slice(&IPV4_ETHERTYPE.to_be_bytes());
        frame[ETH_HEADER_LEN] = 0x45;
        frame[ETH_HEADER_LEN + 2..ETH_HEADER_LEN + 4].copy_from_slice(
            &((IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN + TOKEN_HEADER_LEN) as u16).to_be_bytes(),
        );
        frame[ETH_HEADER_LEN + 9] = UDP_PROTOCOL;
        let udp_offset = ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN;
        frame[udp_offset + 4..udp_offset + 6]
            .copy_from_slice(&((UDP_HEADER_LEN + TOKEN_HEADER_LEN) as u16).to_be_bytes());
        let token_offset = udp_offset + UDP_HEADER_LEN;
        frame[token_offset] = TOKEN_MAGIC;
        frame[token_offset + 1..token_offset + 9].copy_from_slice(&42u64.to_be_bytes());
        frame[token_offset + 9] = expert;
        frame
    }
    #[test]
    fn parser_handles_token_and_variable_ipv4_ihl() {
        let mut frame = token_frame(2);
        let old_len = frame.len();
        frame.splice(
            ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN..ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN,
            [1, 2, 3, 4],
        );
        frame[ETH_HEADER_LEN] = 0x46;
        let ip_total_len = (old_len - ETH_HEADER_LEN + 4) as u16;
        frame[ETH_HEADER_LEN + 2..ETH_HEADER_LEN + 4].copy_from_slice(&ip_total_len.to_be_bytes());
        frame[ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN + 4..ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN + 6]
            .copy_from_slice(&((UDP_HEADER_LEN + TOKEN_HEADER_LEN) as u16).to_be_bytes());
        let parsed = EngineDispatcher::parse_frame(&frame).unwrap();
        assert_eq!(parsed.token_id, 42);
        assert_eq!(parsed.expert_id, 2);
        assert_eq!(parsed.feature_length, 0);
        assert_eq!(
            parsed.payload_offset,
            ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN + 4 + UDP_HEADER_LEN
        );
    }

    #[test]
    fn parser_reports_declared_feature_payload_length() {
        let feature_bytes = [0x12, 0x34, 0x56, 0x78];
        let mut frame = token_frame(1);
        frame.extend_from_slice(&feature_bytes);
        let ip_len =
            (IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN + TOKEN_HEADER_LEN + feature_bytes.len()) as u16;
        frame[ETH_HEADER_LEN + 2..ETH_HEADER_LEN + 4].copy_from_slice(&ip_len.to_be_bytes());
        let udp_offset = ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN;
        let udp_len = (UDP_HEADER_LEN + TOKEN_HEADER_LEN + feature_bytes.len()) as u16;
        frame[udp_offset + 4..udp_offset + 6].copy_from_slice(&udp_len.to_be_bytes());

        let parsed = EngineDispatcher::parse_frame(&frame).unwrap();
        assert_eq!(parsed.feature_length, feature_bytes.len());
    }

    #[test]
    fn full_expert_queue_returns_frame_for_recycling() {
        let (sender, _receiver) = bounded(1);
        let metrics = std::sync::Arc::new(PipelineTelemetry::default());
        let dispatcher = EngineDispatcher::new(vec![sender.clone()], metrics.clone());
        let frame = token_frame(0);
        dispatcher
            .dispatch_frame(FrameDesc::default(), &frame)
            .unwrap();
        let rejected = dispatcher
            .dispatch_frame(FrameDesc::default(), &frame)
            .unwrap_err();
        assert_eq!(rejected.1, FrameDropReason::QueueFull);
        assert_eq!(metrics.saturated_drops_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn parser_rejects_fragmented_ipv4_packet() {
        let mut frame = token_frame(0);
        frame[ETH_HEADER_LEN + 6..ETH_HEADER_LEN + 8].copy_from_slice(&0x2000u16.to_be_bytes());
        assert_eq!(
            EngineDispatcher::parse_frame(&frame),
            Err(FrameDropReason::FragmentedIpv4)
        );
    }

    #[test]
    fn hot_expert_saturation_does_not_block_other_expert_queue() {
        let (hot_sender, _hot_receiver) = bounded(1);
        let (other_sender, other_receiver) = bounded(1);
        let metrics = std::sync::Arc::new(PipelineTelemetry::default());
        let dispatcher = EngineDispatcher::new(vec![hot_sender, other_sender], metrics.clone());
        let hot_frame = token_frame(0);
        let other_frame = token_frame(1);

        dispatcher
            .dispatch_frame(FrameDesc::default(), &hot_frame)
            .unwrap();
        assert_eq!(
            dispatcher
                .dispatch_frame(FrameDesc::default(), &hot_frame)
                .unwrap_err()
                .1,
            FrameDropReason::QueueFull
        );
        dispatcher
            .dispatch_frame(FrameDesc::default(), &other_frame)
            .unwrap();

        assert_eq!(other_receiver.len(), 1);
        assert_eq!(metrics.saturated_drops_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.dispatched_total.load(Ordering::Relaxed), 2);
    }
}
