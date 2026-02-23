"""Tests for production-grade features."""
import pytest
import asyncio
from datetime import datetime, timedelta
import hashlib
import json

from src.models.weights import ModelWeightRegistry, get_model_registry
from src.ingest.deduplication import EventDeduplicator
from src.ingest.circuit_breaker import CircuitBreaker, CircuitBreakerError, CircuitState
from src.security.auth import APIKeyManager, RateLimiter, SignatureValidator
from src.models.event_time import EventTimeWindow, DeterministicTimeProvider


class TestModelWeightRegistry:
    """Test deterministic model weight management."""

    def test_weight_registry_determinism(self):
        """Test that weight registry produces consistent hashes."""
        registry1 = ModelWeightRegistry()
        registry2 = ModelWeightRegistry()

        # Same weights should produce same hash
        assert registry1.get_hash() == registry2.get_hash()

    def test_weight_hash_verification(self):
        """Test hash verification."""
        registry = ModelWeightRegistry()
        correct_hash = registry.get_hash()

        assert registry.verify_hash(correct_hash)
        assert not registry.verify_hash("wrong_hash")

    def test_global_registry_singleton(self):
        """Test global registry is consistent."""
        registry1 = get_model_registry()
        registry2 = get_model_registry()

        assert registry1.get_hash() == registry2.get_hash()

    def test_weight_immutability(self):
        """Test that weights are immutable."""
        registry = ModelWeightRegistry()
        weights1 = registry.get_weights()
        weights2 = registry.get_weights()

        # Modifying returned copy shouldn't affect registry
        weights1["version"] = "999"
        assert registry.get_weights()["version"] == "1.0.0"


class TestEventDeduplicator:
    """Test event deduplication for idempotency."""

    def test_event_id_computation_determinism(self):
        """Test that same inputs produce same event ID."""
        dedup = EventDeduplicator()

        event_id1 = dedup.compute_event_id(
            "sensor-001",
            "2026-02-23T08:00:00Z",
            {"magnitude": 5.0}
        )

        event_id2 = dedup.compute_event_id(
            "sensor-001",
            "2026-02-23T08:00:00Z",
            {"magnitude": 5.0}
        )

        assert event_id1 == event_id2

    def test_event_id_uniqueness(self):
        """Test that different inputs produce different IDs."""
        dedup = EventDeduplicator()

        event_id1 = dedup.compute_event_id(
            "sensor-001",
            "2026-02-23T08:00:00Z",
            {"magnitude": 5.0}
        )

        event_id2 = dedup.compute_event_id(
            "sensor-001",
            "2026-02-23T08:00:01Z",  # Different timestamp
            {"magnitude": 5.0}
        )

        assert event_id1 != event_id2

    def test_duplicate_detection(self):
        """Test duplicate event detection."""
        dedup = EventDeduplicator(window_seconds=60)

        event_id = dedup.compute_event_id(
            "sensor-001",
            "2026-02-23T08:00:00Z",
            {"magnitude": 5.0}
        )

        # First time should not be duplicate
        assert not dedup.is_duplicate(event_id)

        # Second time should be duplicate
        assert dedup.is_duplicate(event_id)

    def test_deduplication_stats(self):
        """Test deduplication statistics."""
        dedup = EventDeduplicator()

        event_id = dedup.compute_event_id("test", "2026-02-23T08:00:00Z", {})
        dedup.is_duplicate(event_id)

        stats = dedup.get_stats()
        assert stats["tracked_events"] == 1


class TestCircuitBreaker:
    """Test circuit breaker for backpressure."""

    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in closed state allows calls."""
        cb = CircuitBreaker(failure_threshold=3)

        def success_func():
            return "success"

        result = cb.call(success_func)
        assert result == "success"
        assert cb.get_state() == CircuitState.CLOSED

    def test_circuit_breaker_opens_on_failures(self):
        """Test circuit breaker opens after threshold failures."""
        cb = CircuitBreaker(failure_threshold=3)

        def failing_func():
            raise ValueError("test error")

        # Trigger failures
        for _ in range(3):
            try:
                cb.call(failing_func)
            except ValueError:
                pass

        # Circuit should now be open
        assert cb.get_state() == CircuitState.OPEN

        # Further calls should be rejected
        with pytest.raises(CircuitBreakerError):
            cb.call(success_func := lambda: "success")

    def test_circuit_breaker_overload_detection(self):
        """Test circuit breaker triggers on overload."""
        cb = CircuitBreaker(
            queue_depth_threshold=100,
            memory_threshold_mb=1000
        )

        # Trigger queue depth overload
        assert cb.check_overload(queue_depth=150, memory_mb=500)
        assert cb.get_state() == CircuitState.OPEN

    def test_circuit_breaker_reset(self):
        """Test manual circuit breaker reset."""
        cb = CircuitBreaker(failure_threshold=1)

        def failing_func():
            raise ValueError("test")

        try:
            cb.call(failing_func)
        except ValueError:
            pass

        assert cb.get_state() == CircuitState.OPEN

        cb.reset()
        assert cb.get_state() == CircuitState.CLOSED


class TestAPIKeyManager:
    """Test API key authentication."""

    def test_add_and_validate_key(self):
        """Test adding and validating API keys."""
        manager = APIKeyManager()

        api_key = "test-key-12345"
        manager.add_key(api_key, "sensor-001")

        # Valid key should return source_id
        source_id = manager.validate_key(api_key)
        assert source_id == "sensor-001"

    def test_invalid_key_rejection(self):
        """Test invalid key rejection."""
        manager = APIKeyManager()

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc:
            manager.validate_key("invalid-key")

        assert exc.value.status_code == 401

    def test_signal_type_permissions(self):
        """Test signal type permission enforcement."""
        manager = APIKeyManager()

        api_key = "test-key"
        manager.add_key(api_key, "sensor-001", allowed_signal_types=["seismic"])

        # Should work for allowed type
        manager.validate_key(api_key, "seismic")

        # Should fail for disallowed type
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc:
            manager.validate_key(api_key, "climate")

        assert exc.value.status_code == 403


class TestRateLimiter:
    """Test rate limiting."""

    def test_rate_limit_allows_within_limit(self):
        """Test requests within limit are allowed."""
        limiter = RateLimiter(
            global_rate=10,
            per_source_rate=5,
            window_seconds=60
        )

        # Should allow requests within limit
        for _ in range(5):
            assert limiter.check_rate_limit("source-001")

    def test_rate_limit_blocks_over_limit(self):
        """Test requests over limit are blocked."""
        limiter = RateLimiter(
            global_rate=100,
            per_source_rate=3,
            window_seconds=60
        )

        # Fill up the limit
        for _ in range(3):
            limiter.check_rate_limit("source-001")

        # Next request should be blocked
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc:
            limiter.check_rate_limit("source-001")

        assert exc.value.status_code == 429

    def test_rate_limit_stats(self):
        """Test rate limiter statistics."""
        limiter = RateLimiter()

        limiter.check_rate_limit("source-001")
        limiter.check_rate_limit("source-002")

        stats = limiter.get_stats()
        assert stats["global_requests_in_window"] == 2
        assert stats["active_sources"] == 2


class TestSignatureValidator:
    """Test HMAC signature validation."""

    def test_signature_generation_and_validation(self):
        """Test signature generation and validation."""
        validator = SignatureValidator("secret-key")

        payload = b'{"test": "data"}'
        signature = validator.generate_signature(payload)

        # Valid signature should pass
        assert validator.validate_signature(payload, signature)

    def test_invalid_signature_rejection(self):
        """Test invalid signature rejection."""
        validator = SignatureValidator("secret-key")

        payload = b'{"test": "data"}'

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc:
            validator.validate_signature(payload, "invalid-signature")

        assert exc.value.status_code == 401

    def test_signature_determinism(self):
        """Test signature generation is deterministic."""
        validator = SignatureValidator("secret-key")

        payload = b'{"test": "data"}'
        sig1 = validator.generate_signature(payload)
        sig2 = validator.generate_signature(payload)

        assert sig1 == sig2


class TestEventTimeWindow:
    """Test event-time windowing for clock independence."""

    def test_event_time_window_basic(self):
        """Test basic event-time window operations."""
        window = EventTimeWindow(window_duration_seconds=3600)

        event_time = datetime(2026, 2, 23, 8, 0, 0)
        window.add_event(event_time, 1.0)
        window.add_event(event_time + timedelta(minutes=30), 2.0)

        # Current time within window
        values = window.get_values_in_window(event_time + timedelta(minutes=45))
        assert len(values) == 2
        assert values == [1.0, 2.0]

    def test_event_time_window_expiration(self):
        """Test events expire from window."""
        window = EventTimeWindow(window_duration_seconds=3600)

        base_time = datetime(2026, 2, 23, 8, 0, 0)
        window.add_event(base_time, 1.0)
        window.add_event(base_time + timedelta(hours=2), 2.0)

        # First event should be expired
        values = window.get_values_in_window(base_time + timedelta(hours=2))
        assert len(values) == 1
        assert values == [2.0]

    def test_deterministic_time_provider_parsing(self):
        """Test deterministic timestamp parsing."""
        provider = DeterministicTimeProvider()

        # Parse ISO8601 timestamps consistently
        dt1 = provider.parse_event_time("2026-02-23T08:00:00Z")
        dt2 = provider.parse_event_time("2026-02-23T08:00:00Z")

        assert dt1 == dt2

    def test_deterministic_window_bounds(self):
        """Test deterministic window computation."""
        provider = DeterministicTimeProvider()

        event_time = datetime(2026, 2, 23, 8, 0, 0)
        start, end = provider.compute_window_bounds(event_time, 3600)

        assert end == event_time
        assert start == event_time - timedelta(seconds=3600)


class TestDeterministicReproducibility:
    """Test deterministic reproducibility across runs."""

    def test_identical_inputs_produce_identical_outputs(self):
        """Test that identical event streams produce identical alerts."""
        from src.normalization import SignalNormalizer
        from src.models import IngestRequest, SignalType
        from datetime import datetime

        # Create two normalizers
        normalizer1 = SignalNormalizer()
        normalizer2 = SignalNormalizer()

        # Same input
        request = IngestRequest(
            source_id="sensor-001",
            signal_type=SignalType.SEISMIC,
            timestamp=datetime(2026, 2, 23, 8, 0, 0),
            payload={"magnitude": 5.0}
        )

        # Should produce identical results
        result1 = normalizer1.normalize(request)
        result2 = normalizer2.normalize(request)

        assert result1.normalized_value == result2.normalized_value
        assert result1.normalization_version == result2.normalization_version

    def test_event_ordering_independence(self):
        """Test that event ordering doesn't affect window calculations."""
        window1 = EventTimeWindow(window_duration_seconds=3600)
        window2 = EventTimeWindow(window_duration_seconds=3600)

        base_time = datetime(2026, 2, 23, 8, 0, 0)

        # Add events in different orders
        window1.add_event(base_time, 1.0)
        window1.add_event(base_time + timedelta(minutes=30), 2.0)

        window2.add_event(base_time + timedelta(minutes=30), 2.0)
        window2.add_event(base_time, 1.0)

        # Should produce same results
        values1 = sorted(window1.get_values_in_window(base_time + timedelta(hours=1)))
        values2 = sorted(window2.get_values_in_window(base_time + timedelta(hours=1)))

        assert values1 == values2
