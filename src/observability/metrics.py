"""Prometheus metrics for observability."""
from prometheus_client import Counter, Histogram, Gauge, Info
import time


# Info metrics
app_info = Info('giewa_app', 'Application information')
app_info.info({
    'version': '1.0.0',
    'model_version': '1.0.0'
})

# Request metrics
ingest_requests_total = Counter(
    'giewa_ingest_requests_total',
    'Total number of ingest requests',
    ['source_type', 'signal_type']
)

ingest_requests_failed = Counter(
    'giewa_ingest_requests_failed_total',
    'Total number of failed ingest requests',
    ['error_type']
)

# Processing metrics
signal_processing_duration_seconds = Histogram(
    'giewa_signal_processing_duration_seconds',
    'Time spent processing signals',
    ['signal_type'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

detection_latency_seconds = Histogram(
    'giewa_detection_latency_seconds',
    'Anomaly detection latency',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5]
)

# Anomaly and alert metrics
anomalies_detected_total = Counter(
    'giewa_anomalies_detected_total',
    'Total number of anomalies detected',
    ['signal_type']
)

alerts_generated_total = Counter(
    'giewa_alerts_generated_total',
    'Total number of alerts generated',
    ['severity']
)

alert_confidence = Histogram(
    'giewa_alert_confidence',
    'Distribution of alert confidence scores',
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0]
)

# False positive tracking
false_positive_rate = Gauge(
    'giewa_false_positive_rate',
    'Estimated false positive rate based on feedback'
)

# Storage metrics
alert_storage_size = Gauge(
    'giewa_alert_storage_size',
    'Number of alerts in storage'
)

alert_storage_capacity = Gauge(
    'giewa_alert_storage_capacity',
    'Maximum alert storage capacity'
)

# Streaming consumer metrics
kafka_consumer_lag = Gauge(
    'giewa_kafka_consumer_lag',
    'Kafka consumer lag',
    ['topic', 'partition']
)

kafka_messages_consumed = Counter(
    'giewa_kafka_messages_consumed_total',
    'Total Kafka messages consumed',
    ['topic']
)

# Queue and backpressure metrics
processing_queue_depth = Gauge(
    'giewa_processing_queue_depth',
    'Current processing queue depth'
)

processing_queue_capacity = Gauge(
    'giewa_processing_queue_capacity',
    'Maximum processing queue capacity'
)

backpressure_events_total = Counter(
    'giewa_backpressure_events_total',
    'Number of backpressure events (circuit breaker activations)'
)

# Event deduplication metrics
duplicate_events_total = Counter(
    'giewa_duplicate_events_total',
    'Total number of duplicate events rejected',
    ['source_id']
)

deduplication_cache_size = Gauge(
    'giewa_deduplication_cache_size',
    'Number of events tracked for deduplication'
)

# System health metrics
circuit_breaker_state = Gauge(
    'giewa_circuit_breaker_state',
    'Circuit breaker state (0=closed, 1=open, 2=half-open)'
)

memory_usage_bytes = Gauge(
    'giewa_memory_usage_bytes',
    'Process memory usage in bytes'
)

# Model execution metrics
model_weights_hash = Info('giewa_model_weights', 'Model weights content hash')


class MetricsCollector:
    """Helper class for collecting metrics with timing."""

    @staticmethod
    def time_signal_processing(signal_type: str):
        """Context manager for timing signal processing."""
        return signal_processing_duration_seconds.labels(signal_type=signal_type).time()

    @staticmethod
    def time_detection():
        """Context manager for timing detection."""
        return detection_latency_seconds.time()

    @staticmethod
    def record_ingest(source_type: str, signal_type: str):
        """Record an ingest request."""
        ingest_requests_total.labels(
            source_type=source_type,
            signal_type=signal_type
        ).inc()

    @staticmethod
    def record_ingest_failure(error_type: str):
        """Record a failed ingest request."""
        ingest_requests_failed.labels(error_type=error_type).inc()

    @staticmethod
    def record_anomaly(signal_type: str):
        """Record an anomaly detection."""
        anomalies_detected_total.labels(signal_type=signal_type).inc()

    @staticmethod
    def record_alert(severity: int, confidence: float):
        """Record an alert generation."""
        alerts_generated_total.labels(severity=str(severity)).inc()
        alert_confidence.observe(confidence)

    @staticmethod
    def record_duplicate(source_id: str):
        """Record a duplicate event."""
        duplicate_events_total.labels(source_id=source_id).inc()

    @staticmethod
    def update_storage_metrics(current_size: int, capacity: int):
        """Update alert storage metrics."""
        alert_storage_size.set(current_size)
        alert_storage_capacity.set(capacity)

    @staticmethod
    def update_queue_metrics(depth: int, capacity: int):
        """Update queue depth metrics."""
        processing_queue_depth.set(depth)
        processing_queue_capacity.set(capacity)

    @staticmethod
    def record_backpressure():
        """Record a backpressure event."""
        backpressure_events_total.inc()

    @staticmethod
    def set_circuit_breaker_state(state: int):
        """Set circuit breaker state (0=closed, 1=open, 2=half-open)."""
        circuit_breaker_state.set(state)
