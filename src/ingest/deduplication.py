"""Event deduplication for idempotent ingestion."""
import hashlib
import time
from typing import Optional, Set
from collections import deque
from threading import Lock
import logging

logger = logging.getLogger(__name__)


class EventDeduplicator:
    """
    Provides exactly-once semantics for event ingestion.
    Tracks processed event IDs to prevent duplicate processing.
    """

    def __init__(self, window_seconds: int = 3600, max_events: int = 100000):
        """
        Initialize event deduplicator.

        Args:
            window_seconds: How long to remember event IDs (default 1 hour)
            max_events: Maximum number of event IDs to track
        """
        self.window_seconds = window_seconds
        self.max_events = max_events

        # Thread-safe tracking
        self._lock = Lock()
        self._seen_events: Set[str] = set()
        self._event_timestamps: deque = deque(maxlen=max_events)

    def compute_event_id(self, source_id: str, timestamp: str, payload: dict) -> str:
        """
        Compute deterministic event ID from signal components.

        Args:
            source_id: Source identifier
            timestamp: ISO8601 timestamp
            payload: Signal payload

        Returns:
            SHA-256 hash as event ID
        """
        import json

        # Create canonical representation
        event_data = {
            "source_id": source_id,
            "timestamp": timestamp,
            "payload": payload,
        }
        canonical = json.dumps(event_data, sort_keys=True, ensure_ascii=True)

        # Hash for ID
        hash_obj = hashlib.sha256(canonical.encode('utf-8'))
        return hash_obj.hexdigest()

    def is_duplicate(self, event_id: str) -> bool:
        """
        Check if event has been processed before.

        Args:
            event_id: Event identifier

        Returns:
            True if duplicate, False if new
        """
        with self._lock:
            # Clean old events first
            self._cleanup_old_events()

            if event_id in self._seen_events:
                logger.warning(f"Duplicate event detected: {event_id[:16]}...")
                return True

            # Mark as seen
            self._seen_events.add(event_id)
            self._event_timestamps.append((event_id, time.time()))

            return False

    def _cleanup_old_events(self):
        """Remove events older than the time window."""
        current_time = time.time()
        cutoff_time = current_time - self.window_seconds

        # Remove old events from the front of the deque
        while self._event_timestamps and self._event_timestamps[0][1] < cutoff_time:
            old_event_id, _ = self._event_timestamps.popleft()
            self._seen_events.discard(old_event_id)

    def mark_processed(self, event_id: str):
        """
        Explicitly mark event as processed (for at-least-once semantics).

        Args:
            event_id: Event identifier
        """
        with self._lock:
            if event_id not in self._seen_events:
                self._seen_events.add(event_id)
                self._event_timestamps.append((event_id, time.time()))

    def get_stats(self) -> dict:
        """Get deduplication statistics."""
        with self._lock:
            return {
                "tracked_events": len(self._seen_events),
                "window_seconds": self.window_seconds,
                "max_events": self.max_events,
            }
