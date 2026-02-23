"""Security middleware for authentication and rate limiting."""
import time
import hashlib
import hmac
from typing import Optional, Dict
from collections import defaultdict
from threading import Lock
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
import logging

logger = logging.getLogger(__name__)

# API Key header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class APIKeyManager:
    """
    Manages API key authentication.
    In production, this would integrate with a secure key store.
    """

    def __init__(self):
        """Initialize API key manager."""
        self._keys: Dict[str, dict] = {}
        self._lock = Lock()

    def add_key(
        self,
        api_key: str,
        source_id: str,
        allowed_signal_types: Optional[list] = None
    ):
        """
        Add an API key.

        Args:
            api_key: The API key
            source_id: Associated source identifier
            allowed_signal_types: List of allowed signal types (None = all)
        """
        with self._lock:
            # Hash the key for storage
            key_hash = hashlib.sha256(api_key.encode()).hexdigest()
            self._keys[key_hash] = {
                "source_id": source_id,
                "allowed_signal_types": allowed_signal_types,
                "created_at": time.time(),
            }

    def validate_key(self, api_key: str, signal_type: Optional[str] = None) -> str:
        """
        Validate API key and return source_id.

        Args:
            api_key: The API key to validate
            signal_type: Signal type being ingested

        Returns:
            source_id if valid

        Raises:
            HTTPException: If key is invalid
        """
        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API key required"
            )

        key_hash = hashlib.sha256(api_key.encode()).hexdigest()

        with self._lock:
            if key_hash not in self._keys:
                logger.warning(f"Invalid API key attempt: {key_hash[:16]}...")
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid API key"
                )

            key_info = self._keys[key_hash]

            # Check signal type permissions
            if signal_type and key_info["allowed_signal_types"]:
                if signal_type not in key_info["allowed_signal_types"]:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"API key not authorized for signal type: {signal_type}"
                    )

            return key_info["source_id"]


class RateLimiter:
    """
    Token bucket rate limiter with per-source limits.
    """

    def __init__(
        self,
        global_rate: int = 1000,
        per_source_rate: int = 100,
        window_seconds: int = 60
    ):
        """
        Initialize rate limiter.

        Args:
            global_rate: Global requests per window
            per_source_rate: Per-source requests per window
            window_seconds: Time window in seconds
        """
        self.global_rate = global_rate
        self.per_source_rate = per_source_rate
        self.window_seconds = window_seconds

        self._lock = Lock()
        self._global_requests: list = []
        self._source_requests: Dict[str, list] = defaultdict(list)

    def check_rate_limit(self, source_id: str) -> bool:
        """
        Check if request is within rate limits.

        Args:
            source_id: Source identifier

        Returns:
            True if allowed, False if rate limited

        Raises:
            HTTPException: If rate limit exceeded
        """
        current_time = time.time()
        cutoff_time = current_time - self.window_seconds

        with self._lock:
            # Clean old requests
            self._global_requests = [
                t for t in self._global_requests if t > cutoff_time
            ]
            self._source_requests[source_id] = [
                t for t in self._source_requests[source_id] if t > cutoff_time
            ]

            # Check global limit
            if len(self._global_requests) >= self.global_rate:
                logger.warning(f"Global rate limit exceeded: {len(self._global_requests)}")
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Global rate limit exceeded",
                    headers={"Retry-After": str(self.window_seconds)}
                )

            # Check per-source limit
            source_count = len(self._source_requests[source_id])
            if source_count >= self.per_source_rate:
                logger.warning(
                    f"Per-source rate limit exceeded for {source_id}: {source_count}"
                )
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit exceeded for source {source_id}",
                    headers={"Retry-After": str(self.window_seconds)}
                )

            # Record request
            self._global_requests.append(current_time)
            self._source_requests[source_id].append(current_time)

            return True

    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        current_time = time.time()
        cutoff_time = current_time - self.window_seconds

        with self._lock:
            # Clean old requests
            self._global_requests = [
                t for t in self._global_requests if t > cutoff_time
            ]

            active_sources = {
                source: len([t for t in times if t > cutoff_time])
                for source, times in self._source_requests.items()
            }

            return {
                "global_requests_in_window": len(self._global_requests),
                "global_rate_limit": self.global_rate,
                "active_sources": len(active_sources),
                "window_seconds": self.window_seconds,
            }


class SignatureValidator:
    """
    HMAC signature validation for signal sources.
    """

    def __init__(self, secret_key: str):
        """
        Initialize signature validator.

        Args:
            secret_key: Shared secret for HMAC
        """
        self.secret_key = secret_key.encode()

    def generate_signature(self, payload: bytes) -> str:
        """
        Generate HMAC signature for payload.

        Args:
            payload: Request payload bytes

        Returns:
            Hexadecimal signature
        """
        signature = hmac.new(
            self.secret_key,
            payload,
            hashlib.sha256
        ).hexdigest()
        return signature

    def validate_signature(self, payload: bytes, signature: str) -> bool:
        """
        Validate HMAC signature.

        Args:
            payload: Request payload bytes
            signature: Provided signature

        Returns:
            True if valid

        Raises:
            HTTPException: If signature is invalid
        """
        expected_signature = self.generate_signature(payload)

        # Use constant-time comparison to prevent timing attacks
        if not hmac.compare_digest(expected_signature, signature):
            logger.warning("Invalid signature detected")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid signature"
            )

        return True


# Global instances
_api_key_manager = APIKeyManager()
_rate_limiter = RateLimiter()


def get_api_key_manager() -> APIKeyManager:
    """Get global API key manager."""
    return _api_key_manager


def get_rate_limiter() -> RateLimiter:
    """Get global rate limiter."""
    return _rate_limiter
