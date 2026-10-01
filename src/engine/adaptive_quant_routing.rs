use std::sync::atomic::{AtomicU64, Ordering};

pub use super::elastic_quant::{
    CongestionAction, ElasticQuantizer, QuantizationError, QuantizedFeatures,
};

pub const NUM_EXPERTS: usize = 8;
pub const FALLBACK_EXPERT_ID: usize = NUM_EXPERTS - 1;
pub const SATURATION_WATERMARK_PERCENT: u8 = 90;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteError {
    InvalidCapacity,
    InvalidExpert,
    QueueFull,
}

pub struct AdaptiveRouter {
    queue_capacities: [usize; NUM_EXPERTS],
    queue_depths: [usize; NUM_EXPERTS],
    fallback_routed_total: AtomicU64,
    saturated_drops_total: AtomicU64,
}

impl AdaptiveRouter {
    pub fn new(capacity: usize) -> Result<Self, RouteError> {
        if capacity == 0 {
            return Err(RouteError::InvalidCapacity);
        }
        Ok(Self {
            queue_capacities: [capacity; NUM_EXPERTS],
            queue_depths: [0; NUM_EXPERTS],
            fallback_routed_total: AtomicU64::new(0),
            saturated_drops_total: AtomicU64::new(0),
        })
    }

    pub fn route_token(&mut self, requested_expert: u8) -> Result<u8, RouteError> {
        let primary_index = usize::from(requested_expert);
        if primary_index >= NUM_EXPERTS {
            return Err(RouteError::InvalidExpert);
        }

        let primary_available = if primary_index == FALLBACK_EXPERT_ID {
            self.queue_depths[primary_index] < self.queue_capacities[primary_index]
        } else {
            !self.is_saturated(primary_index)
        };
        if primary_available {
            self.queue_depths[primary_index] += 1;
            return Ok(requested_expert);
        }

        if primary_index != FALLBACK_EXPERT_ID
            && self.queue_depths[FALLBACK_EXPERT_ID] < self.queue_capacities[FALLBACK_EXPERT_ID]
        {
            self.queue_depths[FALLBACK_EXPERT_ID] += 1;
            self.fallback_routed_total.fetch_add(1, Ordering::Relaxed);
            return Ok(FALLBACK_EXPERT_ID as u8);
        }

        self.saturated_drops_total.fetch_add(1, Ordering::Relaxed);
        Err(RouteError::QueueFull)
    }

    pub fn release_token(&mut self, expert_id: u8) -> Result<(), RouteError> {
        let expert_index = usize::from(expert_id);
        let Some(depth) = self.queue_depths.get_mut(expert_index) else {
            return Err(RouteError::InvalidExpert);
        };
        *depth = depth.saturating_sub(1);
        Ok(())
    }

    pub fn queue_depth(&self, expert_id: usize) -> Option<usize> {
        self.queue_depths.get(expert_id).copied()
    }

    pub fn fallback_routed_total(&self) -> u64 {
        self.fallback_routed_total.load(Ordering::Relaxed)
    }

    pub fn saturated_drops_total(&self) -> u64 {
        self.saturated_drops_total.load(Ordering::Relaxed)
    }

    fn is_saturated(&self, expert_id: usize) -> bool {
        (self.queue_depths[expert_id] as u128) * 100
            >= (self.queue_capacities[expert_id] as u128) * u128::from(SATURATION_WATERMARK_PERCENT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reroutes_at_watermark_and_tracks_queue_ownership() {
        let mut router = AdaptiveRouter::new(10).unwrap();
        for _ in 0..9 {
            assert_eq!(router.route_token(1), Ok(1));
        }
        assert_eq!(router.route_token(1), Ok(FALLBACK_EXPERT_ID as u8));
        assert_eq!(router.queue_depth(1), Some(9));
        assert_eq!(router.queue_depth(FALLBACK_EXPERT_ID), Some(1));
        assert_eq!(router.fallback_routed_total(), 1);

        router.release_token(FALLBACK_EXPERT_ID as u8).unwrap();
        assert_eq!(router.queue_depth(FALLBACK_EXPERT_ID), Some(0));
    }

    #[test]
    fn rejects_invalid_experts_and_full_fallback_queues() {
        let mut router = AdaptiveRouter::new(1).unwrap();
        assert_eq!(router.route_token(8), Err(RouteError::InvalidExpert));
        assert_eq!(router.route_token(0), Ok(0));
        assert_eq!(router.route_token(7), Ok(7));
        assert_eq!(router.route_token(0), Err(RouteError::QueueFull));
        assert_eq!(router.saturated_drops_total(), 1);
    }

    #[test]
    fn fallback_expert_uses_capacity_until_full() {
        let mut router = AdaptiveRouter::new(10).unwrap();
        for _ in 0..10 {
            assert_eq!(router.route_token(FALLBACK_EXPERT_ID as u8), Ok(7));
        }
        assert_eq!(
            router.route_token(FALLBACK_EXPERT_ID as u8),
            Err(RouteError::QueueFull)
        );
    }

    #[test]
    fn exposes_the_existing_quantizer_through_the_routing_module() {
        let quantizer = ElasticQuantizer::new(90, None).unwrap();
        let result = quantizer.quantize_fp32(&[-1.0, 1.0]).unwrap();
        assert_eq!(result.values, vec![-127, 127]);
    }
}
