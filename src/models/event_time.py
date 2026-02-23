"""Event-time windowing for clock-independent processing."""
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class EventTimeWindow:
    """
    Event-time based windowing for clock-independent processing.
    Uses event timestamps instead of system clock for deterministic behavior.
    """

    def __init__(self, window_duration_seconds: int = 3600):
        """
        Initialize event-time window.

        Args:
            window_duration_seconds: Duration of the time window in seconds
        """
        self.window_duration = timedelta(seconds=window_duration_seconds)
        self._events: Deque[Tuple[datetime, float]] = deque()

    def add_event(self, event_time: datetime, value: float):
        """
        Add event with its timestamp.

        Args:
            event_time: Event timestamp (from event, not system clock)
            value: Event value
        """
        # Maintain sorted order by event time
        self._events.append((event_time, value))

    def get_values_in_window(self, current_event_time: datetime) -> list:
        """
        Get all values within the time window relative to current event time.

        Args:
            current_event_time: Current event timestamp

        Returns:
            List of values within the window
        """
        cutoff_time = current_event_time - self.window_duration

        # Remove events older than the window
        while self._events and self._events[0][0] < cutoff_time:
            self._events.popleft()

        # Return values in window
        return [value for timestamp, value in self._events]

    def clear_before(self, timestamp: datetime):
        """
        Remove all events before given timestamp.

        Args:
            timestamp: Cutoff timestamp
        """
        while self._events and self._events[0][0] < timestamp:
            self._events.popleft()

    def __len__(self) -> int:
        """Return number of events in window."""
        return len(self._events)


class DeterministicTimeProvider:
    """
    Provides consistent time handling for reproducibility.
    Uses event timestamps instead of system clock.
    """

    @staticmethod
    def parse_event_time(timestamp_str: str) -> datetime:
        """
        Parse event timestamp in a deterministic way.

        Args:
            timestamp_str: ISO8601 timestamp string

        Returns:
            Parsed datetime
        """
        # Ensure consistent parsing regardless of system timezone
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'

        dt = datetime.fromisoformat(timestamp_str)

        # Normalize to UTC for determinism
        if dt.tzinfo is None:
            from datetime import timezone
            dt = dt.replace(tzinfo=timezone.utc)

        return dt

    @staticmethod
    def compute_window_bounds(
        event_time: datetime,
        window_size_seconds: int
    ) -> Tuple[datetime, datetime]:
        """
        Compute deterministic window bounds based on event time.

        Args:
            event_time: Event timestamp
            window_size_seconds: Window size in seconds

        Returns:
            Tuple of (start_time, end_time)
        """
        window_delta = timedelta(seconds=window_size_seconds)
        start_time = event_time - window_delta
        end_time = event_time

        return start_time, end_time

    @staticmethod
    def is_within_window(
        event_time: datetime,
        window_start: datetime,
        window_end: datetime
    ) -> bool:
        """
        Check if event is within time window.

        Args:
            event_time: Event timestamp
            window_start: Window start time
            window_end: Window end time

        Returns:
            True if event is within window
        """
        return window_start <= event_time <= window_end
