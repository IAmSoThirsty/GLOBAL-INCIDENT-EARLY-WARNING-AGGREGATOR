"""Signal normalization component."""
from typing import Dict, Any
import numpy as np
from datetime import datetime

from src.models import SignalType, IngestRequest, NormalizedSignal


class SignalNormalizer:
    """
    Normalizes incoming signals to a standard format and scale.
    Different signal types require different normalization strategies.
    """

    VERSION = "1.0.0"

    def __init__(self):
        """Initialize the signal normalizer with type-specific handlers."""
        self._normalizers = {
            SignalType.SEISMIC: self._normalize_seismic,
            SignalType.EPIDEMIOLOGICAL: self._normalize_epidemiological,
            SignalType.CLIMATE: self._normalize_climate,
            SignalType.SATELLITE: self._normalize_satellite,
        }

    def normalize(self, request: IngestRequest) -> NormalizedSignal:
        """
        Normalize an ingested signal.

        Args:
            request: The ingestion request containing raw signal data

        Returns:
            NormalizedSignal with normalized value in range [0, 1]
        """
        normalizer_func = self._normalizers.get(request.signal_type)
        if not normalizer_func:
            raise ValueError(f"Unknown signal type: {request.signal_type}")

        normalized_value = normalizer_func(request.payload)

        return NormalizedSignal(
            source_id=request.source_id,
            signal_type=request.signal_type,
            timestamp=request.timestamp,
            normalized_value=normalized_value,
            raw_payload=request.payload,
            normalization_version=self.VERSION,
        )

    def _normalize_seismic(self, payload: Dict[str, Any]) -> float:
        """
        Normalize seismic signal data.
        Uses magnitude on Richter scale (0-10) as primary indicator.
        """
        magnitude = payload.get("magnitude", 0.0)
        # Normalize Richter scale (0-10) to [0, 1]
        # Apply logarithmic scaling since Richter is already logarithmic
        normalized = np.clip(magnitude / 10.0, 0.0, 1.0)
        return float(normalized)

    def _normalize_epidemiological(self, payload: Dict[str, Any]) -> float:
        """
        Normalize epidemiological signal data.
        Considers infection rate, case count, and growth rate.
        """
        # Extract key metrics
        infection_rate = payload.get("infection_rate", 0.0)  # Expected 0-1
        cases_per_100k = payload.get("cases_per_100k", 0.0)
        growth_rate = payload.get("growth_rate", 0.0)  # Expected as percentage

        # Normalize components
        norm_infection = np.clip(infection_rate, 0.0, 1.0)
        norm_cases = np.clip(cases_per_100k / 1000.0, 0.0, 1.0)  # Cap at 1000/100k
        norm_growth = np.clip(abs(growth_rate) / 100.0, 0.0, 1.0)  # Cap at 100%

        # Weighted combination
        normalized = 0.4 * norm_infection + 0.3 * norm_cases + 0.3 * norm_growth
        return float(np.clip(normalized, 0.0, 1.0))

    def _normalize_climate(self, payload: Dict[str, Any]) -> float:
        """
        Normalize climate signal data.
        Considers temperature anomalies, precipitation, and extreme events.
        """
        temp_anomaly = payload.get("temperature_anomaly_celsius", 0.0)
        precip_anomaly = payload.get("precipitation_anomaly_percent", 0.0)
        wind_speed_kmh = payload.get("wind_speed_kmh", 0.0)

        # Normalize components
        # Temperature anomaly: -10°C to +10°C normalized
        norm_temp = np.clip((temp_anomaly + 10.0) / 20.0, 0.0, 1.0)
        # Precipitation anomaly: -100% to +200% normalized
        norm_precip = np.clip((precip_anomaly + 100.0) / 300.0, 0.0, 1.0)
        # Wind speed: 0-200 km/h normalized
        norm_wind = np.clip(wind_speed_kmh / 200.0, 0.0, 1.0)

        # Calculate deviation from normal (0.5)
        deviations = [abs(x - 0.5) for x in [norm_temp, norm_precip, norm_wind]]
        normalized = 0.5 + max(deviations)

        return float(np.clip(normalized, 0.0, 1.0))

    def _normalize_satellite(self, payload: Dict[str, Any]) -> float:
        """
        Normalize satellite signal data.
        Processes various satellite observations like land change, heat, etc.
        """
        # Extract satellite metrics
        ndvi_change = payload.get("ndvi_change", 0.0)  # Vegetation index change
        thermal_anomaly = payload.get("thermal_anomaly_kelvin", 0.0)
        change_detection = payload.get("change_detection_score", 0.0)  # Expected 0-1

        # Normalize NDVI change: -1 to +1
        norm_ndvi = np.clip((abs(ndvi_change)), 0.0, 1.0)
        # Thermal anomaly: -50K to +50K
        norm_thermal = np.clip(abs(thermal_anomaly) / 50.0, 0.0, 1.0)
        # Change detection score
        norm_change = np.clip(change_detection, 0.0, 1.0)

        # Weighted combination
        normalized = 0.3 * norm_ndvi + 0.3 * norm_thermal + 0.4 * norm_change
        return float(np.clip(normalized, 0.0, 1.0))
