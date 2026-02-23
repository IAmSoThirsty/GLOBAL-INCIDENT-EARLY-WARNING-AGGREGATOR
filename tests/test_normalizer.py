"""Test signal normalization."""
import pytest
from datetime import datetime

from src.models import IngestRequest, SignalType
from src.normalization import SignalNormalizer


class TestSignalNormalizer:
    """Test cases for SignalNormalizer."""

    def setup_method(self):
        """Set up test fixtures."""
        self.normalizer = SignalNormalizer()

    def test_normalize_seismic(self):
        """Test seismic signal normalization."""
        request = IngestRequest(
            source_id="sensor-001",
            signal_type=SignalType.SEISMIC,
            timestamp=datetime.utcnow(),
            payload={"magnitude": 5.0, "depth_km": 10}
        )

        result = self.normalizer.normalize(request)

        assert result.source_id == "sensor-001"
        assert result.signal_type == SignalType.SEISMIC
        assert 0.0 <= result.normalized_value <= 1.0
        assert result.normalized_value == 0.5  # 5.0 / 10.0

    def test_normalize_epidemiological(self):
        """Test epidemiological signal normalization."""
        request = IngestRequest(
            source_id="health-dept-001",
            signal_type=SignalType.EPIDEMIOLOGICAL,
            timestamp=datetime.utcnow(),
            payload={
                "infection_rate": 0.5,
                "cases_per_100k": 100,
                "growth_rate": 10,
            }
        )

        result = self.normalizer.normalize(request)

        assert result.source_id == "health-dept-001"
        assert 0.0 <= result.normalized_value <= 1.0

    def test_normalize_climate(self):
        """Test climate signal normalization."""
        request = IngestRequest(
            source_id="weather-station-001",
            signal_type=SignalType.CLIMATE,
            timestamp=datetime.utcnow(),
            payload={
                "temperature_anomaly_celsius": 5.0,
                "precipitation_anomaly_percent": 50.0,
                "wind_speed_kmh": 80.0,
            }
        )

        result = self.normalizer.normalize(request)

        assert result.source_id == "weather-station-001"
        assert 0.0 <= result.normalized_value <= 1.0

    def test_normalize_satellite(self):
        """Test satellite signal normalization."""
        request = IngestRequest(
            source_id="satellite-001",
            signal_type=SignalType.SATELLITE,
            timestamp=datetime.utcnow(),
            payload={
                "ndvi_change": 0.3,
                "thermal_anomaly_kelvin": 10.0,
                "change_detection_score": 0.7,
            }
        )

        result = self.normalizer.normalize(request)

        assert result.source_id == "satellite-001"
        assert 0.0 <= result.normalized_value <= 1.0

    def test_normalization_version(self):
        """Test that normalization version is recorded."""
        request = IngestRequest(
            source_id="test",
            signal_type=SignalType.SEISMIC,
            timestamp=datetime.utcnow(),
            payload={"magnitude": 3.0}
        )

        result = self.normalizer.normalize(request)
        assert result.normalization_version == "1.0.0"
