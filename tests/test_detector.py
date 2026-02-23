"""Test anomaly detection."""
import pytest
from datetime import datetime

from src.models import NormalizedSignal, SignalType
from src.detection import AnomalyDetector


class TestAnomalyDetector:
    """Test cases for AnomalyDetector."""

    def setup_method(self):
        """Set up test fixtures."""
        self.detector = AnomalyDetector(window_size=50, z_threshold=3.0)

    def test_detect_with_insufficient_history(self):
        """Test detection with insufficient historical data."""
        signal = NormalizedSignal(
            source_id="test-sensor",
            signal_type=SignalType.SEISMIC,
            timestamp=datetime.utcnow(),
            normalized_value=0.5,
            raw_payload={},
        )

        is_anomaly, score, metadata = self.detector.detect(signal)

        assert not is_anomaly
        assert score == 0.0
        assert "insufficient_history" in metadata["reason"]

    def test_detect_normal_signal(self):
        """Test detection of normal signals."""
        # Feed normal data
        for i in range(20):
            signal = NormalizedSignal(
                source_id="test-sensor",
                signal_type=SignalType.SEISMIC,
                timestamp=datetime.utcnow(),
                normalized_value=0.5 + (i % 3) * 0.01,  # Small variations
                raw_payload={},
            )
            is_anomaly, score, metadata = self.detector.detect(signal)

        # Last signal should not be anomalous
        assert not is_anomaly or score < 0.5

    def test_detect_anomalous_signal(self):
        """Test detection of anomalous signals."""
        # Feed normal data
        for i in range(20):
            signal = NormalizedSignal(
                source_id="test-sensor",
                signal_type=SignalType.SEISMIC,
                timestamp=datetime.utcnow(),
                normalized_value=0.3,
                raw_payload={},
            )
            self.detector.detect(signal)

        # Feed anomalous data
        anomalous_signal = NormalizedSignal(
            source_id="test-sensor",
            signal_type=SignalType.SEISMIC,
            timestamp=datetime.utcnow(),
            normalized_value=0.95,  # Significantly different
            raw_payload={},
        )

        is_anomaly, score, metadata = self.detector.detect(anomalous_signal)

        assert score > 0.0
        assert "ensemble_score" in metadata
        assert "individual_scores" in metadata

    def test_model_version(self):
        """Test that model version is accessible."""
        version = self.detector.get_model_version()
        assert version == "1.0.0"
