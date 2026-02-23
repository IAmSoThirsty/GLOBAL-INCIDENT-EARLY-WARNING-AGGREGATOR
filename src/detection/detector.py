"""Multi-model anomaly detection layer."""
from typing import List, Dict, Any, Tuple
import numpy as np
from scipy import stats
from datetime import datetime, timedelta
from collections import deque

from src.models import NormalizedSignal, SignalType


class AnomalyDetector:
    """
    Multi-model statistical anomaly detection.
    Uses multiple detection algorithms for robustness.
    """

    VERSION = "1.0.0"

    def __init__(self, window_size: int = 100, z_threshold: float = 3.0):
        """
        Initialize the anomaly detector.

        Args:
            window_size: Number of historical signals to maintain for statistics
            z_threshold: Z-score threshold for anomaly detection
        """
        self.window_size = window_size
        self.z_threshold = z_threshold
        self._history: Dict[str, deque] = {}  # source_id -> deque of values
        self._model_weights = self._load_model_weights()

    def _load_model_weights(self) -> Dict[str, float]:
        """Load versioned model weights for different detection methods."""
        return {
            "z_score": 0.35,
            "iqr": 0.25,
            "moving_average": 0.20,
            "rate_of_change": 0.20,
        }

    def detect(self, signal: NormalizedSignal) -> Tuple[bool, float, Dict[str, Any]]:
        """
        Detect if a signal is anomalous using multiple models.

        Args:
            signal: Normalized signal to analyze

        Returns:
            Tuple of (is_anomaly, anomaly_score, detection_metadata)
            - is_anomaly: Boolean indicating if signal is anomalous
            - anomaly_score: Score in [0, 1] indicating strength of anomaly
            - detection_metadata: Dict with detailed detection information
        """
        # Initialize history for new sources
        source_key = f"{signal.source_id}_{signal.signal_type.value}"
        if source_key not in self._history:
            self._history[source_key] = deque(maxlen=self.window_size)

        history = self._history[source_key]

        # Need minimum samples for statistical detection
        if len(history) < 10:
            history.append(signal.normalized_value)
            return False, 0.0, {"reason": "insufficient_history", "samples": len(history)}

        # Run multiple detection models
        scores = {}
        scores["z_score"] = self._z_score_detection(signal.normalized_value, history)
        scores["iqr"] = self._iqr_detection(signal.normalized_value, history)
        scores["moving_average"] = self._moving_average_detection(
            signal.normalized_value, history
        )
        scores["rate_of_change"] = self._rate_of_change_detection(
            signal.normalized_value, history
        )

        # Weighted ensemble score
        ensemble_score = sum(
            scores[method] * self._model_weights[method]
            for method in scores
        )

        # Update history
        history.append(signal.normalized_value)

        # Determine if anomaly (threshold at 0.5)
        is_anomaly = ensemble_score > 0.5

        metadata = {
            "model_version": self.VERSION,
            "individual_scores": scores,
            "ensemble_score": ensemble_score,
            "model_weights": self._model_weights,
            "history_size": len(history),
        }

        return is_anomaly, ensemble_score, metadata

    def _z_score_detection(self, value: float, history: deque) -> float:
        """
        Z-score based anomaly detection.
        Returns normalized score [0, 1].
        """
        data = np.array(history)
        mean = np.mean(data)
        std = np.std(data)

        if std == 0:
            return 0.0

        z_score = abs((value - mean) / std)
        # Normalize z-score to [0, 1] using sigmoid
        normalized_score = 1 / (1 + np.exp(-0.5 * (z_score - self.z_threshold)))
        return float(normalized_score)

    def _iqr_detection(self, value: float, history: deque) -> float:
        """
        Interquartile range (IQR) based outlier detection.
        Returns normalized score [0, 1].
        """
        data = np.array(history)
        q1, q3 = np.percentile(data, [25, 75])
        iqr = q3 - q1

        if iqr == 0:
            return 0.0

        # Distance from IQR bounds
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        if value < lower_bound:
            distance = (lower_bound - value) / iqr
        elif value > upper_bound:
            distance = (value - upper_bound) / iqr
        else:
            return 0.0

        # Normalize distance to [0, 1]
        normalized_score = min(distance / 3.0, 1.0)
        return float(normalized_score)

    def _moving_average_detection(self, value: float, history: deque) -> float:
        """
        Moving average deviation detection.
        Returns normalized score [0, 1].
        """
        data = np.array(history)
        ma_window = min(20, len(data))
        moving_avg = np.mean(data[-ma_window:])

        # Calculate deviation from moving average
        deviation = abs(value - moving_avg)

        # Normalize using historical standard deviation
        std = np.std(data)
        if std == 0:
            return 0.0

        normalized_deviation = deviation / (2 * std)  # 2 std deviations as reference
        return float(min(normalized_deviation, 1.0))

    def _rate_of_change_detection(self, value: float, history: deque) -> float:
        """
        Rate of change (velocity) based detection.
        Detects sudden jumps in signal value.
        Returns normalized score [0, 1].
        """
        if len(history) < 2:
            return 0.0

        # Calculate rate of change
        previous_value = history[-1]
        rate_of_change = abs(value - previous_value)

        # Historical rate of change
        data = np.array(history)
        historical_changes = np.abs(np.diff(data))

        if len(historical_changes) == 0:
            return 0.0

        mean_change = np.mean(historical_changes)
        std_change = np.std(historical_changes)

        if std_change == 0:
            return float(min(rate_of_change / (mean_change + 0.01), 1.0))

        # Z-score of rate of change
        change_z_score = (rate_of_change - mean_change) / std_change
        normalized_score = 1 / (1 + np.exp(-0.5 * (change_z_score - 2.0)))
        return float(normalized_score)

    def get_model_version(self) -> str:
        """Return the current model version for reproducibility."""
        return self.VERSION
