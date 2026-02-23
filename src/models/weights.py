"""Model weight management with deterministic hashing."""
import hashlib
import json
from typing import Dict, Any
import numpy as np


class ModelWeightRegistry:
    """
    Immutable, hash-addressed model weight registry.
    Ensures deterministic reproducibility across distributed replicas.
    """

    def __init__(self):
        """Initialize the registry with versioned model weights."""
        # Seed numpy for deterministic operations
        np.random.seed(42)

        self._weights = self._define_model_weights()
        self._hash = self._compute_hash(self._weights)

    def _define_model_weights(self) -> Dict[str, Any]:
        """
        Define immutable model weights.
        These weights are versioned and content-addressed.
        """
        return {
            "version": "1.0.0",
            "detector_weights": {
                "z_score": 0.35,
                "iqr": 0.25,
                "moving_average": 0.20,
                "rate_of_change": 0.20,
            },
            "confidence_params": {
                "confidence_level": 0.95,
                "min_bound_width": 0.05,
            },
            "normalization_params": {
                "seismic_max": 10.0,
                "epidemiological_case_cap": 1000.0,
                "climate_temp_range": 20.0,
                "satellite_thermal_range": 50.0,
            },
        }

    def _compute_hash(self, weights: Dict[str, Any]) -> str:
        """
        Compute SHA-256 hash of model weights for content addressing.

        Args:
            weights: Dictionary of model weights

        Returns:
            Hexadecimal hash string
        """
        # Serialize weights deterministically
        canonical_json = json.dumps(weights, sort_keys=True, ensure_ascii=True)
        hash_obj = hashlib.sha256(canonical_json.encode('utf-8'))
        return hash_obj.hexdigest()

    def get_weights(self) -> Dict[str, Any]:
        """Get immutable copy of model weights."""
        return self._weights.copy()

    def get_hash(self) -> str:
        """Get content hash of model weights."""
        return self._hash

    def verify_hash(self, expected_hash: str) -> bool:
        """
        Verify that current weights match expected hash.

        Args:
            expected_hash: Expected SHA-256 hash

        Returns:
            True if hash matches, False otherwise
        """
        return self._hash == expected_hash

    def get_detector_weights(self) -> Dict[str, float]:
        """Get detector ensemble weights."""
        return self._weights["detector_weights"].copy()

    def get_version(self) -> str:
        """Get model version."""
        return self._weights["version"]


# Global singleton registry
_model_registry = ModelWeightRegistry()


def get_model_registry() -> ModelWeightRegistry:
    """Get the global model weight registry."""
    return _model_registry
