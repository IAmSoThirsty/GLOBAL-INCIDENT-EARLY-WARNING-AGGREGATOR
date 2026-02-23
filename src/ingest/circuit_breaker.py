"""Circuit breaker pattern for backpressure management."""
import time
from enum import Enum
from threading import Lock
import logging

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = 0  # Normal operation
    OPEN = 1    # Rejecting requests
    HALF_OPEN = 2  # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker for protecting system from overload.

    Monitors failure rate and queue depth to prevent cascading failures.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        queue_depth_threshold: int = 1000,
        memory_threshold_mb: int = 1024,
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            queue_depth_threshold: Max queue depth before triggering
            memory_threshold_mb: Max memory usage in MB before triggering
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.queue_depth_threshold = queue_depth_threshold
        self.memory_threshold_mb = memory_threshold_mb

        self._lock = Lock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0
        self._last_state_change = time.time()

    def call(self, func, *args, **kwargs):
        """
        Execute function through circuit breaker.

        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            CircuitBreakerError: If circuit is open
        """
        with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if we should try recovery
                if time.time() - self._last_state_change >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._last_state_change = time.time()
                    logger.info("Circuit breaker entering HALF_OPEN state")
                else:
                    raise CircuitBreakerError("Circuit breaker is OPEN")

        # Execute function
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        """Handle successful execution."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                # Recovery successful, close circuit
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._last_state_change = time.time()
                logger.info("Circuit breaker CLOSED after successful recovery")
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = max(0, self._failure_count - 1)

    def _on_failure(self):
        """Handle failed execution."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Recovery failed, reopen circuit
                self._state = CircuitState.OPEN
                self._last_state_change = time.time()
                logger.warning("Circuit breaker OPEN after failed recovery attempt")
            elif self._state == CircuitState.CLOSED:
                # Check if we should open circuit
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitState.OPEN
                    self._last_state_change = time.time()
                    logger.error(
                        f"Circuit breaker OPEN after {self._failure_count} failures"
                    )

    def check_overload(self, queue_depth: int, memory_mb: float) -> bool:
        """
        Check system overload conditions.

        Args:
            queue_depth: Current queue depth
            memory_mb: Current memory usage in MB

        Returns:
            True if system is overloaded
        """
        if queue_depth >= self.queue_depth_threshold:
            logger.warning(
                f"Queue depth threshold exceeded: {queue_depth} >= "
                f"{self.queue_depth_threshold}"
            )
            self._trigger_overload()
            return True

        if memory_mb >= self.memory_threshold_mb:
            logger.warning(
                f"Memory threshold exceeded: {memory_mb}MB >= "
                f"{self.memory_threshold_mb}MB"
            )
            self._trigger_overload()
            return True

        return False

    def _trigger_overload(self):
        """Trigger circuit breaker due to overload."""
        with self._lock:
            if self._state != CircuitState.OPEN:
                self._state = CircuitState.OPEN
                self._last_state_change = time.time()
                logger.error("Circuit breaker OPEN due to system overload")

    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            return self._state

    def get_state_value(self) -> int:
        """Get circuit state as integer for metrics."""
        return self.get_state().value

    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self.get_state() == CircuitState.OPEN

    def reset(self):
        """Manually reset circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._last_state_change = time.time()
            logger.info("Circuit breaker manually RESET")

    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "state": self._state.name,
                "state_value": self._state.value,
                "failure_count": self._failure_count,
                "time_in_current_state": time.time() - self._last_state_change,
            }


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass
