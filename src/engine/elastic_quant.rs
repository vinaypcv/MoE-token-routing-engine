#[derive(Debug, Clone, PartialEq)]
pub struct QuantizedFeatures {
    pub values: Vec<i8>,
    pub scale: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CongestionAction {
    PreservePrecision,
    Quantize,
    Fallback { expert_id: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantizationError {
    InvalidFeatureBytes,
    EmptyFeatures,
}

impl std::fmt::Display for QuantizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidFeatureBytes => "invalid quantizer configuration",
            Self::EmptyFeatures => "cannot quantize an empty feature vector",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for QuantizationError {}

pub struct ElasticQuantizer {
    high_watermark_percent: u8,
    fallback_expert: Option<usize>,
}

impl ElasticQuantizer {
    pub fn new(
        high_watermark_percent: u8,
        fallback_expert: Option<usize>,
    ) -> Result<Self, QuantizationError> {
        if high_watermark_percent == 0 || high_watermark_percent > 100 {
            return Err(QuantizationError::InvalidFeatureBytes);
        }
        Ok(Self {
            high_watermark_percent,
            fallback_expert,
        })
    }

    pub fn action(&self, queue_depth: usize, queue_capacity: usize) -> CongestionAction {
        if queue_capacity == 0
            || queue_depth.saturating_mul(100)
                < queue_capacity * self.high_watermark_percent as usize
        {
            CongestionAction::PreservePrecision
        } else if let Some(expert_id) = self.fallback_expert {
            CongestionAction::Fallback { expert_id }
        } else {
            CongestionAction::Quantize
        }
    }

    pub fn quantize_fp32(&self, features: &[f32]) -> Result<QuantizedFeatures, QuantizationError> {
        if features.is_empty() {
            return Err(QuantizationError::EmptyFeatures);
        }
        let max_abs = features
            .iter()
            .map(|value| value.abs())
            .fold(0.0_f32, f32::max);
        let scale = if max_abs == 0.0 { 1.0 } else { max_abs / 127.0 };
        let values = if cfg!(target_arch = "x86_64") && is_x86_feature_detected!("avx2") {
            // SAFETY: the runtime feature check above guarantees AVX2 support.
            unsafe { quantize_avx2(features, scale) }
        } else {
            quantize_scalar(features, scale)
        };
        Ok(QuantizedFeatures { values, scale })
    }
}

fn quantize_scalar(features: &[f32], scale: f32) -> Vec<i8> {
    features
        .iter()
        .map(|value| (value / scale).round().clamp(-128.0, 127.0) as i8)
        .collect()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn quantize_avx2(features: &[f32], scale: f32) -> Vec<i8> {
    use std::arch::x86_64::{_mm256_cvtps_epi32, _mm256_loadu_ps, _mm256_mul_ps, _mm256_set1_ps};

    let mut values = Vec::with_capacity(features.len());
    let scale_vector = _mm256_set1_ps(1.0 / scale);
    let (chunks, remainder) = features.as_chunks::<8>();
    for chunk in chunks {
        let scaled = _mm256_mul_ps(_mm256_loadu_ps(chunk.as_ptr()), scale_vector);
        let rounded = _mm256_cvtps_epi32(scaled);
        let mut lanes = [0i32; 8];
        _mm256_storeu_si256(lanes.as_mut_ptr().cast(), rounded);
        values.extend(lanes.map(|value| value.clamp(-128, 127) as i8));
    }
    values.extend(quantize_scalar(remainder, scale));
    values
}

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::_mm256_storeu_si256;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantizes_with_symmetric_scale_without_mutating_input() {
        let quantizer = ElasticQuantizer::new(90, None).unwrap();
        let input = [-2.0, 0.0, 2.0];
        let output = quantizer.quantize_fp32(&input).unwrap();
        assert_eq!(input, [-2.0, 0.0, 2.0]);
        assert_eq!(output.values, vec![-127, 0, 127]);
        assert!((output.scale - (2.0 / 127.0)).abs() < f32::EPSILON);
    }

    #[test]
    fn selects_quantization_then_fallback_at_high_watermark() {
        let quantizer = ElasticQuantizer::new(90, None).unwrap();
        assert_eq!(
            quantizer.action(899, 1_000),
            CongestionAction::PreservePrecision
        );
        assert_eq!(quantizer.action(900, 1_000), CongestionAction::Quantize);

        let fallback = ElasticQuantizer::new(90, Some(7)).unwrap();
        assert_eq!(
            fallback.action(900, 1_000),
            CongestionAction::Fallback { expert_id: 7 }
        );
    }
}
