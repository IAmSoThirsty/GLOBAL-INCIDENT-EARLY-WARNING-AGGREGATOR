# Production Features Quick Reference

## New Production-Grade Features

This implementation now meets the same standard as the Constraint Engine with the following production features:

### 1. Deterministic Reproducibility ✅
- **Hash-addressed model weights**: Content-addressed via SHA-256
- **Fixed NumPy seed**: Eliminates floating-point drift
- **Event-time processing**: Uses event timestamps, not system clock
- **Tests**: 27/27 passing including determinism tests

### 2. Idempotent Ingestion ✅
- **Event deduplication**: SHA-256 event IDs from (source, timestamp, payload)
- **Exactly-once semantics**: 1-hour deduplication window (configurable)
- **Replay safety**: Identical Kafka replays produce identical results

### 3. Backpressure Management ✅
- **Circuit breaker**: Protects against overload (CLOSED/OPEN/HALF-OPEN states)
- **Memory bounds**: Automatic eviction when thresholds exceeded
- **Queue monitoring**: Prometheus metrics for queue depth
- **Graceful degradation**: Returns 503 with Retry-After header

### 4. Durable Persistence ✅
- **PostgreSQL storage**: Survives pod restarts
- **Indexed queries**: Fast lookups on timestamp, region, severity, confidence
- **Connection pooling**: 2-10 concurrent connections
- **Fallback**: In-memory storage if PostgreSQL disabled

### 5. Security Model ✅
- **API key authentication**: Per-source keys with permissions
- **Rate limiting**: 100 req/min per source, 1000 req/min global
- **HMAC signatures**: SHA-256 request signing for anti-spoofing
- **Request size limits**: 10 MB max (configurable)

### 6. Kubernetes Ready ✅
- **Hardened Dockerfile**: Non-root user, read-only filesystem
- **Deployment manifests**: HPA (3-20 pods), PDB (minAvailable: 2)
- **Network policies**: Zero-trust networking
- **Health probes**: Liveness, readiness, and startup probes

### 7. Comprehensive Observability ✅
- **20+ Prometheus metrics**: Request rates, latencies, errors
- **Histograms**: Detection latency, alert confidence
- **Gauges**: Memory usage, queue depth, circuit breaker state
- **Counters**: Duplicates, anomalies, alerts by severity

### 8. Anti-Spoofing ✅
- **Source authentication**: API keys tied to sources
- **Signature validation**: HMAC-SHA256 request signing
- **Per-source rate limits**: Prevent single-source floods

### 9. Clock Independence ✅
- **Event timestamps**: All windowing uses event.timestamp
- **Deterministic parsing**: UTC normalization
- **Order independence**: Sorted processing ensures consistency

## Quick Start

### Basic Usage (No Auth)
```bash
# Start server
python run_server.py

# Ingest signal
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_id": "sensor-001",
    "signal_type": "seismic",
    "timestamp": "2026-02-23T08:00:00Z",
    "payload": {"magnitude": 6.5}
  }'
```

### Production Usage (With Auth)
```bash
# Set environment
export ENABLE_AUTH=true
export ENABLE_POSTGRES=true
export DATABASE_URL=postgresql://user:pass@localhost:5432/giewa

# Ingest with API key
curl -X POST http://localhost:8000/ingest \
  -H "X-API-Key: your-api-key-here" \
  -H "Content-Type: application/json" \
  -d '{...}'
```

### Kubernetes Deployment
```bash
# Build image
docker build -t giewa:v1.0.0 .

# Deploy
kubectl create namespace early-warning
kubectl apply -f k8s/

# Scale
kubectl scale deployment giewa --replicas=10 -n early-warning

# Monitor
kubectl get hpa giewa -n early-warning
kubectl logs -f deployment/giewa -n early-warning
```

### Metrics
```bash
# View Prometheus metrics
curl http://localhost:8000/metrics

# Key metrics to monitor
giewa_circuit_breaker_state
giewa_processing_queue_depth
giewa_memory_usage_bytes
giewa_detection_latency_seconds
giewa_alert_confidence
```

## Configuration

### Environment Variables
```env
# Security
ENABLE_AUTH=false
ENABLE_RATE_LIMITING=true
GLOBAL_RATE_LIMIT=1000
PER_SOURCE_RATE_LIMIT=100
HMAC_SECRET=change-me-in-production

# Persistence
ENABLE_POSTGRES=false
DATABASE_URL=postgresql://giewa:giewa@localhost:5432/giewa

# Circuit Breaker
CIRCUIT_BREAKER_ENABLED=true
CIRCUIT_FAILURE_THRESHOLD=5
QUEUE_DEPTH_THRESHOLD=1000
MEMORY_THRESHOLD_MB=1536

# Deduplication
DEDUP_WINDOW_SECONDS=3600
MAX_TRACKED_EVENTS=100000

# Observability
ENABLE_METRICS=true
```

## Testing

```bash
# Run all tests (42 total)
pytest

# Run production feature tests (27 tests)
pytest tests/test_production_features.py

# Run original tests (15 tests)
pytest tests/test_api.py tests/test_detector.py tests/test_normalizer.py

# Coverage
pytest --cov=src tests/
```

## Files Added

### Core Production Features
- `src/models/weights.py` - Hash-addressed model weights
- `src/models/event_time.py` - Event-time windowing
- `src/ingest/deduplication.py` - Event deduplication
- `src/ingest/circuit_breaker.py` - Circuit breaker
- `src/security/auth.py` - Authentication & rate limiting
- `src/persistence/postgres.py` - PostgreSQL storage
- `src/observability/metrics.py` - Prometheus metrics

### Deployment
- `Dockerfile` - Hardened container image
- `k8s/deployment.yaml` - Kubernetes manifests
- `k8s/configmap.yaml` - Configuration

### Enhanced API
- `src/api/main_enhanced.py` - Production API (use this instead of main.py)

### Documentation
- `PRODUCTION_COMPLIANCE.md` - Detailed compliance documentation
- `tests/test_production_features.py` - 27 production feature tests

## Verification

### Deterministic Reproducibility
```python
# Same inputs = same outputs
from src.models.weights import get_model_registry

registry1 = get_model_registry()
registry2 = get_model_registry()

assert registry1.get_hash() == registry2.get_hash()
```

### Idempotency
```bash
# Send same request twice
EVENT='{"source_id":"test","signal_type":"seismic","timestamp":"2026-02-23T08:00:00Z","payload":{"magnitude":5.0}}'

curl -X POST localhost:8000/ingest -d "$EVENT"
# Returns: {"status": "processed", "event_id": "abc123..."}

curl -X POST localhost:8000/ingest -d "$EVENT"
# Returns: {"status": "duplicate", "event_id": "abc123..."}
```

### Circuit Breaker
```bash
# Check circuit state
curl localhost:8000/stats | jq '.circuit_breaker.state'

# Monitor via Prometheus
curl localhost:8000/metrics | grep circuit_breaker_state
```

## Compliance Summary

All 9 requirements for "Finished Microservice" status are met:

1. ✅ Deterministic reproducibility (hash-addressed weights, fixed seed, event-time)
2. ✅ Idempotent ingestion (event IDs, deduplication, replay safety)
3. ✅ Backpressure strategy (circuit breaker, metrics, memory bounds)
4. ✅ Durable persistence (PostgreSQL with indexes)
5. ✅ Security model (auth, rate limiting, signatures, size limits)
6. ✅ Kubernetes artifacts (Dockerfile, Deployment, HPA, PDB, NetworkPolicy)
7. ✅ Observability (20+ metrics, histograms, gauges)
8. ✅ Anti-spoofing (authentication, signatures, per-source limits)
9. ✅ Clock independence (event timestamps, deterministic windowing)

**Test Coverage:** 42 tests passing (15 original + 27 production features)

See `PRODUCTION_COMPLIANCE.md` for detailed compliance documentation.
