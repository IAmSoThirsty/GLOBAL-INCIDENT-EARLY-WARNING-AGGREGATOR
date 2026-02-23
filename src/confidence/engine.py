"""Confidence scoring and interval estimation engine."""
from typing import Tuple, Dict, Any
import numpy as np
from scipy import stats

from src.models import ConfidenceBounds


class ConfidenceEngine:
    """
    Computes confidence scores and intervals for alerts.
    Uses statistical methods to estimate reliability of detections.
    """

    VERSION = "1.0.0"

    def __init__(self, confidence_level: float = 0.95):
        """
        Initialize the confidence engine.

        Args:
            confidence_level: Statistical confidence level (default 95%)
        """
        self.confidence_level = confidence_level

    def compute_confidence(
        self,
        anomaly_score: float,
        detection_metadata: Dict[str, Any],
        signal_count: int = 1,
    ) -> Tuple[float, ConfidenceBounds]:
        """
        Compute confidence score and bounds for an alert.

        Args:
            anomaly_score: Raw anomaly score from detector [0, 1]
            detection_metadata: Metadata from detection layer
            signal_count: Number of correlated signals (default 1)

        Returns:
            Tuple of (confidence_score, confidence_bounds)
        """
        # Base confidence from anomaly score
        base_confidence = anomaly_score

        # Adjust for ensemble agreement
        individual_scores = detection_metadata.get("individual_scores", {})
        if individual_scores:
            ensemble_agreement = self._calculate_ensemble_agreement(individual_scores)
            base_confidence = 0.7 * base_confidence + 0.3 * ensemble_agreement

        # Boost confidence with correlated signals
        correlation_boost = self._calculate_correlation_boost(signal_count)
        adjusted_confidence = min(base_confidence * correlation_boost, 1.0)

        # Penalize if insufficient history
        history_size = detection_metadata.get("history_size", 0)
        history_penalty = self._calculate_history_penalty(history_size)
        final_confidence = adjusted_confidence * history_penalty

        # Compute confidence bounds
        bounds = self._compute_confidence_bounds(
            final_confidence,
            history_size,
            len(individual_scores) if individual_scores else 1,
        )

        return float(np.clip(final_confidence, 0.0, 1.0)), bounds

    def _calculate_ensemble_agreement(self, individual_scores: Dict[str, float]) -> float:
        """
        Calculate agreement among individual detection models.
        Higher agreement = higher confidence.
        """
        if not individual_scores:
            return 0.5

        scores = list(individual_scores.values())

        # Calculate variance in scores (lower variance = higher agreement)
        variance = np.var(scores)

        # Calculate mean score
        mean_score = np.mean(scores)

        # Agreement metric: combine low variance with high mean
        # Lower variance -> higher agreement
        agreement = mean_score * (1 - min(variance, 1.0))

        return float(agreement)

    def _calculate_correlation_boost(self, signal_count: int) -> float:
        """
        Calculate confidence boost from correlated signals.
        More correlated signals = higher confidence.
        """
        if signal_count <= 1:
            return 1.0

        # Logarithmic boost: diminishing returns after multiple signals
        boost = 1.0 + 0.15 * np.log1p(signal_count - 1)
        return float(min(boost, 1.5))  # Cap at 50% boost

    def _calculate_history_penalty(self, history_size: int) -> float:
        """
        Apply penalty for insufficient historical data.
        More history = more reliable detection.
        """
        if history_size >= 100:
            return 1.0
        elif history_size >= 50:
            return 0.95
        elif history_size >= 20:
            return 0.85
        elif history_size >= 10:
            return 0.75
        else:
            return 0.5

    def _compute_confidence_bounds(
        self,
        confidence: float,
        sample_size: int,
        num_models: int,
    ) -> ConfidenceBounds:
        """
        Compute confidence interval bounds.

        Args:
            confidence: Point estimate of confidence
            sample_size: Historical sample size
            num_models: Number of models in ensemble

        Returns:
            ConfidenceBounds object with lower and upper bounds
        """
        # Effective sample size considers both history and ensemble
        effective_n = max(sample_size, 10) * max(num_models, 1) / 10

        # Standard error estimation
        # Use beta distribution assumption for bounded [0,1] confidence
        alpha = max(confidence * effective_n, 1)
        beta = max((1 - confidence) * effective_n, 1)

        # Calculate credible interval using beta distribution
        lower_bound = stats.beta.ppf((1 - self.confidence_level) / 2, alpha, beta)
        upper_bound = stats.beta.ppf(1 - (1 - self.confidence_level) / 2, alpha, beta)

        # Ensure bounds are valid
        lower_bound = float(np.clip(lower_bound, 0.0, confidence))
        upper_bound = float(np.clip(upper_bound, confidence, 1.0))

        # Minimum bound width for uncertainty representation
        min_width = 0.05
        current_width = upper_bound - lower_bound
        if current_width < min_width:
            adjustment = (min_width - current_width) / 2
            lower_bound = max(0.0, lower_bound - adjustment)
            upper_bound = min(1.0, upper_bound + adjustment)

        return ConfidenceBounds(
            lower=lower_bound,
            upper=upper_bound,
            confidence_level=self.confidence_level,
        )

    def get_version(self) -> str:
        """Return the confidence engine version."""
        return self.VERSION
