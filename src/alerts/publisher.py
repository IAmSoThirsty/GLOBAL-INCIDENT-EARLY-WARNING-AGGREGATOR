"""Alert publishing and storage component."""
from typing import List, Optional
from datetime import datetime
from collections import deque
import threading

from src.models import Alert, AlertQueryParams, NormalizedSignal, SignalType


class AlertPublisher:
    """
    Manages alert emission and storage.
    Maintains an in-memory store of recent alerts for querying.
    """

    def __init__(self, max_alerts: int = 10000):
        """
        Initialize the alert publisher.

        Args:
            max_alerts: Maximum number of alerts to keep in memory
        """
        self.max_alerts = max_alerts
        self._alerts: deque = deque(maxlen=max_alerts)
        self._lock = threading.Lock()
        self._signal_correlations: dict = {}  # incident_id -> list of signal_ids

    def publish(self, alert: Alert) -> str:
        """
        Publish an alert to the system.

        Args:
            alert: Alert object to publish

        Returns:
            incident_id of the published alert
        """
        with self._lock:
            self._alerts.append(alert)

            # Store signal correlations for future queries
            if alert.signal_correlations:
                self._signal_correlations[alert.incident_id] = alert.signal_correlations

        return alert.incident_id

    def query_alerts(self, params: AlertQueryParams) -> List[Alert]:
        """
        Query alerts based on filter parameters.

        Args:
            params: Query parameters for filtering alerts

        Returns:
            List of alerts matching the criteria
        """
        with self._lock:
            filtered_alerts = list(self._alerts)

        # Apply filters
        if params.region:
            filtered_alerts = [
                a for a in filtered_alerts
                if a.region == params.region
            ]

        if params.min_severity is not None:
            filtered_alerts = [
                a for a in filtered_alerts
                if a.severity >= params.min_severity
            ]

        if params.min_confidence is not None:
            filtered_alerts = [
                a for a in filtered_alerts
                if a.confidence >= params.min_confidence
            ]

        if params.start_time is not None:
            filtered_alerts = [
                a for a in filtered_alerts
                if a.timestamp >= params.start_time
            ]

        if params.end_time is not None:
            filtered_alerts = [
                a for a in filtered_alerts
                if a.timestamp <= params.end_time
            ]

        # Sort by timestamp (most recent first)
        filtered_alerts.sort(key=lambda a: a.timestamp, reverse=True)

        # Apply limit
        return filtered_alerts[:params.limit]

    def get_alert_by_id(self, incident_id: str) -> Optional[Alert]:
        """
        Retrieve a specific alert by incident ID.

        Args:
            incident_id: The incident ID to search for

        Returns:
            Alert if found, None otherwise
        """
        with self._lock:
            for alert in self._alerts:
                if alert.incident_id == incident_id:
                    return alert
        return None

    def get_correlated_signals(self, incident_id: str) -> List[str]:
        """
        Get signal IDs correlated with an incident.

        Args:
            incident_id: The incident ID

        Returns:
            List of correlated signal IDs
        """
        return self._signal_correlations.get(incident_id, [])

    def get_stats(self) -> dict:
        """
        Get statistics about stored alerts.

        Returns:
            Dictionary with alert statistics
        """
        with self._lock:
            total = len(self._alerts)
            if total == 0:
                return {
                    "total_alerts": 0,
                    "avg_severity": 0,
                    "avg_confidence": 0,
                }

            severities = [a.severity for a in self._alerts]
            confidences = [a.confidence for a in self._alerts]

            return {
                "total_alerts": total,
                "avg_severity": sum(severities) / total,
                "avg_confidence": sum(confidences) / total,
                "max_severity": max(severities),
                "min_confidence": min(confidences),
                "max_confidence": max(confidences),
            }
