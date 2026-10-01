#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExpertPrediction {
    pub expert_id: usize,
    pub confidence: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredictionError {
    EmptyFeatures,
    InvalidTopK,
    LowConfidence,
}

impl std::fmt::Display for PredictionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::EmptyFeatures => "prediction requires non-empty features",
            Self::InvalidTopK => "prediction top-k is invalid",
            Self::LowConfidence => "prediction confidence is below threshold",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for PredictionError {}

#[derive(Clone)]
pub struct TokenAwarePredictor {
    expert_count: usize,
    top_k: usize,
    confidence_threshold: f32,
}

impl TokenAwarePredictor {
    pub fn new(
        expert_count: usize,
        top_k: usize,
        confidence_threshold: f32,
    ) -> Result<Self, PredictionError> {
        if expert_count == 0 || top_k == 0 || top_k > expert_count {
            return Err(PredictionError::InvalidTopK);
        }
        Ok(Self {
            expert_count,
            top_k,
            confidence_threshold: confidence_threshold.clamp(0.0, 1.0),
        })
    }

    pub fn predict(
        &self,
        features: &[u8],
        queue_depths: &[u64],
    ) -> Result<Vec<ExpertPrediction>, PredictionError> {
        if features.is_empty() {
            return Err(PredictionError::EmptyFeatures);
        }
        let mut scores: Vec<(usize, f32)> = (0..self.expert_count)
            .map(|expert_id| {
                let mut score = 0.0f32;
                for (feature_index, feature) in features.iter().enumerate() {
                    let rotation = ((expert_id + feature_index) % 8) as u32;
                    score += f32::from(feature.rotate_left(rotation)) / 255.0;
                }
                let queue_penalty = queue_depths.get(expert_id).copied().unwrap_or(0) as f32;
                (
                    expert_id,
                    score / features.len() as f32 - queue_penalty * 0.001,
                )
            })
            .collect();
        scores.sort_by(|left, right| right.1.total_cmp(&left.1));
        let best_score = scores[0].1.max(0.0001);
        let predictions = scores
            .into_iter()
            .take(self.top_k)
            .map(|(expert_id, score)| ExpertPrediction {
                expert_id,
                confidence: (score.max(0.0) / best_score).clamp(0.0, 1.0),
            })
            .collect::<Vec<_>>();
        if predictions[0].confidence < self.confidence_threshold {
            return Err(PredictionError::LowConfidence);
        }
        Ok(predictions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_top_k_predictions_and_avoids_busy_expert() {
        let predictor = TokenAwarePredictor::new(4, 2, 0.0).unwrap();
        let predictions = predictor
            .predict(&[10, 40, 90, 200], &[10_000, 0, 0, 0])
            .unwrap();
        assert_eq!(predictions.len(), 2);
        assert!(!predictions
            .iter()
            .any(|prediction| prediction.expert_id == 0));
    }

    #[test]
    fn rejects_empty_features_and_invalid_top_k() {
        assert!(matches!(
            TokenAwarePredictor::new(2, 3, 0.5),
            Err(PredictionError::InvalidTopK)
        ));
        let predictor = TokenAwarePredictor::new(2, 1, 0.5).unwrap();
        assert_eq!(
            predictor.predict(&[], &[]),
            Err(PredictionError::EmptyFeatures)
        );
    }
}
