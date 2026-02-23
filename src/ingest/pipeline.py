"""Data ingestion pipeline orchestrator."""
from typing import Optional
import logging

from src.models import IngestRequest, NormalizedSignal, Alert
from src.normalization import SignalNormalizer
from src.detection import AnomalyDetector
from src.confidence import ConfidenceEngine
from src.alerts import AlertPublisher

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Orchestrates the complete signal ingestion pipeline:
    1. Signal normalization
    2. Anomaly detection
    3. Confidence scoring
    4. Alert generation and publishing
    """

    def __init__(
        self,
        normalizer: Optional[SignalNormalizer] = None,
        detector: Optional[AnomalyDetector] = None,
        confidence_engine: Optional[ConfidenceEngine] = None,
        alert_publisher: Optional[AlertPublisher] = None,
    ):
        """
        Initialize the ingestion pipeline.

        Args:
            normalizer: Signal normalizer instance
            detector: Anomaly detector instance
            confidence_engine: Confidence scoring engine instance
            alert_publisher: Alert publisher instance
        """
        self.normalizer = normalizer or SignalNormalizer()
        self.detector = detector or AnomalyDetector()
        self.confidence_engine = confidence_engine or ConfidenceEngine()
        self.alert_publisher = alert_publisher or AlertPublisher()

        # Statistics
        self._signals_processed = 0
        self._anomalies_detected = 0
        self._alerts_generated = 0

    async def process_signal(self, request: IngestRequest) -> Optional[Alert]:
        """
        Process an incoming signal through the complete pipeline.

        Args:
            request: Ingestion request containing raw signal data

        Returns:
            Alert object if anomaly detected, None otherwise
        """
        try:
            # Step 1: Normalize signal
            normalized_signal = self.normalizer.normalize(request)
            logger.info(
                f"Normalized signal {normalized_signal.signal_id} "
                f"from {request.source_id}: {normalized_signal.normalized_value:.3f}"
            )

            self._signals_processed += 1

            # Step 2: Detect anomalies
            is_anomaly, anomaly_score, detection_metadata = self.detector.detect(
                normalized_signal
            )

            if not is_anomaly:
                logger.debug(
                    f"Signal {normalized_signal.signal_id} is normal "
                    f"(score: {anomaly_score:.3f})"
                )
                return None

            self._anomalies_detected += 1
            logger.warning(
                f"Anomaly detected in signal {normalized_signal.signal_id} "
                f"(score: {anomaly_score:.3f})"
            )

            # Step 3: Calculate confidence
            confidence_score, confidence_bounds = self.confidence_engine.compute_confidence(
                anomaly_score=anomaly_score,
                detection_metadata=detection_metadata,
                signal_count=1,  # Single signal for now, can be enhanced with correlation
            )

            # Step 4: Determine severity
            severity = self._calculate_severity(
                anomaly_score=anomaly_score,
                confidence=confidence_score,
                signal_type=normalized_signal.signal_type,
            )

            # Step 5: Generate alert
            alert = Alert(
                severity=severity,
                confidence=confidence_score,
                confidence_bounds=confidence_bounds,
                region=self._extract_region(normalized_signal),
                signal_correlations=[normalized_signal.signal_id],
                recommended_action=self._generate_recommended_action(
                    signal_type=normalized_signal.signal_type,
                    severity=severity,
                ),
                model_version=self.detector.get_model_version(),
                detection_metadata=detection_metadata,
            )

            # Step 6: Publish alert
            incident_id = self.alert_publisher.publish(alert)
            self._alerts_generated += 1

            logger.info(
                f"Alert {incident_id} generated with severity {severity} "
                f"and confidence {confidence_score:.3f}"
            )

            return alert

        except Exception as e:
            logger.error(f"Error processing signal: {e}", exc_info=True)
            raise

    def _calculate_severity(
        self,
        anomaly_score: float,
        confidence: float,
        signal_type: str,
    ) -> int:
        """
        Calculate alert severity based on anomaly score and confidence.

        Returns:
            Severity level 1-5
        """
        # Combined score considering both anomaly and confidence
        combined_score = (anomaly_score + confidence) / 2

        if combined_score >= 0.9:
            return 5  # Critical
        elif combined_score >= 0.75:
            return 4  # High
        elif combined_score >= 0.6:
            return 3  # Medium
        elif combined_score >= 0.4:
            return 2  # Low
        else:
            return 1  # Minimal

    def _extract_region(self, signal: NormalizedSignal) -> str:
        """
        Extract region from signal payload.
        Default implementation, can be enhanced with geolocation.
        """
        payload = signal.raw_payload

        # Try to extract from common location fields
        if "region" in payload:
            return payload["region"]
        elif "latitude" in payload and "longitude" in payload:
            return f"lat{payload['latitude']:.1f}_lon{payload['longitude']:.1f}"
        else:
            return f"source_{signal.source_id}"

    def _generate_recommended_action(self, signal_type: str, severity: int) -> str:
        """
        Generate recommended action based on signal type and severity.
        """
        actions = {
            "seismic": {
                5: "Evacuate affected areas immediately. Activate emergency response.",
                4: "Monitor seismic activity and prepare for potential aftershocks.",
                3: "Alert local authorities and monitor situation.",
                2: "Continue monitoring seismic sensors.",
                1: "Log event for future analysis.",
            },
            "epidemiological": {
                5: "Initiate pandemic response protocols. Implement containment measures.",
                4: "Increase surveillance and prepare healthcare facilities.",
                3: "Monitor outbreak progression and contact trace.",
                2: "Enhance disease surveillance in affected region.",
                1: "Document case and continue routine monitoring.",
            },
            "climate": {
                5: "Issue severe weather warning. Evacuate high-risk areas.",
                4: "Prepare emergency services and issue weather advisory.",
                3: "Monitor weather patterns and alert local authorities.",
                2: "Continue climate monitoring and prepare for changes.",
                1: "Log climate anomaly for trend analysis.",
            },
            "satellite": {
                5: "Dispatch emergency teams to affected area immediately.",
                4: "Verify satellite observation with ground teams.",
                3: "Investigate reported anomaly with additional sensors.",
                2: "Schedule follow-up satellite observation.",
                1: "Log observation for future reference.",
            },
        }

        default_action = f"Investigate anomaly and take appropriate action based on severity level {severity}."
        return actions.get(signal_type, {}).get(severity, default_action)

    def get_stats(self) -> dict:
        """
        Get pipeline statistics.

        Returns:
            Dictionary with processing statistics
        """
        return {
            "signals_processed": self._signals_processed,
            "anomalies_detected": self._anomalies_detected,
            "alerts_generated": self._alerts_generated,
            "detection_rate": (
                self._anomalies_detected / self._signals_processed
                if self._signals_processed > 0
                else 0
            ),
        }
