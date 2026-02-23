# Global Incident Early Warning Aggregator

Aggregate multi-source telemetry and detect anomaly patterns for early incident warning.

## Overview

This system provides real-time anomaly detection and alerting across multiple signal types including seismic, epidemiological, climate, and satellite data. It uses statistical multi-model detection to identify anomalous patterns and generate alerts with confidence intervals.

## Core Responsibilities

- **Data ingestion pipelines**: HTTP API and streaming (Kafka/NATS) ingestion
- **Signal normalization**: Type-specific normalization to [0, 1] scale
- **Statistical anomaly detection**: Multi-model ensemble detection (Z-score, IQR, moving average, rate of change)
- **Alert emission**: Versioned, reproducible alert generation
- **Confidence interval modeling**: Statistical confidence bounds for all alerts

## Architecture

```
Streaming Ingest (Kafka/NATS) ──┐
                                 ├──> Signal Normalizer ──> Multi-Model Detection Layer ──> Confidence Scoring Engine ──> Alert Publisher
HTTP API (POST /ingest) ────────┘
```

## API Surface

### POST /ingest

Ingest a signal for anomaly detection processing.

**Request:**
```json
{
  "source_id": "string",
  "signal_type": "seismic | epidemiological | climate | satellite",
  "timestamp": "iso8601",
  "payload": {...}
}
```

**Signal-Specific Payloads:**

**Seismic:**
```json
{
  "magnitude": 5.2,
  "depth_km": 10,
  "latitude": 35.6762,
  "longitude": 139.6503
}
```

**Epidemiological:**
```json
{
  "infection_rate": 0.15,
  "cases_per_100k": 250,
  "growth_rate": 15.5
}
```

**Climate:**
```json
{
  "temperature_anomaly_celsius": 3.5,
  "precipitation_anomaly_percent": 75,
  "wind_speed_kmh": 120
}
```

**Satellite:**
```json
{
  "ndvi_change": -0.4,
  "thermal_anomaly_kelvin": 15,
  "change_detection_score": 0.85
}
```

**Response:**
```json
{
  "status": "processed",
  "anomaly_detected": true,
  "alert_generated": true,
  "incident_id": "uuid",
  "severity": 4,
  "confidence": 0.87
}
```

### GET /alerts

Query alerts with optional filters.

**Query Parameters:**
- `region`: Filter by region
- `min_severity`: Minimum severity (1-5)
- `min_confidence`: Minimum confidence (0.0-1.0)
- `start_time`: Start of time range (ISO8601)
- `end_time`: End of time range (ISO8601)
- `limit`: Maximum results (default: 100, max: 1000)

**Response:**
```json
[
  {
    "incident_id": "uuid",
    "severity": 4,
    "confidence": 0.87,
    "confidence_bounds": {
      "lower": 0.82,
      "upper": 0.92,
      "confidence_level": 0.95
    },
    "region": "string",
    "signal_correlations": ["sig-001", "sig-002"],
    "recommended_action": "string",
    "timestamp": "iso8601",
    "model_version": "1.0.0"
  }
]
```

### GET /alerts/{incident_id}

Retrieve a specific alert by ID.

### GET /stats

Get system statistics.

### GET /health

Health check endpoint.

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# For development and testing
pip install -r requirements-dev.txt
```

## Configuration

Create a `.env` file or set environment variables:

```env
# API Settings
API_HOST=0.0.0.0
API_PORT=8000
API_RELOAD=false

# Pipeline Settings
ANOMALY_WINDOW_SIZE=100
ANOMALY_Z_THRESHOLD=3.0
CONFIDENCE_LEVEL=0.95
MAX_ALERTS=10000

# Streaming Settings
ENABLE_KAFKA=false
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=signals
KAFKA_GROUP_ID=early-warning-aggregator

ENABLE_NATS=false
NATS_SERVERS=nats://localhost:4222
NATS_SUBJECT=signals.*

# Logging
LOG_LEVEL=INFO
```

## Running the System

### HTTP API Server

```bash
python run_server.py
```

The API will be available at `http://localhost:8000`. Visit `http://localhost:8000/docs` for interactive API documentation.

### Streaming Consumers

```bash
python run_streaming.py
```

Enable Kafka or NATS in configuration before running.

## Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_api.py

# Run with coverage
pytest --cov=src tests/
```

## Invariants

- **All alerts include confidence bounds**: Every alert has statistical confidence intervals
- **Model weights versioned**: Detection model weights are versioned for reproducibility
- **Alert generation reproducible**: Same inputs produce same alerts (given model version)

## Failure Modes

### False Positives
- Mitigated by ensemble detection (multiple algorithms must agree)
- Confidence scoring provides uncertainty quantification
- Tunable thresholds for different signal types

### Signal Spoofing
- Input validation on all ingestion endpoints
- Signal correlation helps identify inconsistent patterns
- Historical pattern analysis for anomaly detection

### Ingest Overload
- Asynchronous processing pipeline
- Bounded in-memory storage (configurable max alerts)
- Streaming consumers support horizontal scaling

## Components

### Signal Normalizer (`src/normalization/`)
Normalizes different signal types to [0, 1] scale using type-specific algorithms.

### Anomaly Detector (`src/detection/`)
Multi-model statistical anomaly detection:
- Z-score based detection
- IQR (Interquartile Range) outlier detection
- Moving average deviation
- Rate of change detection

Ensemble scoring combines all models with configurable weights.

### Confidence Engine (`src/confidence/`)
Computes confidence scores and statistical bounds:
- Ensemble agreement analysis
- Signal correlation boosting
- History-based reliability estimation
- Beta distribution for credible intervals

### Alert Publisher (`src/alerts/`)
Manages alert storage and querying:
- In-memory alert store (bounded)
- Multi-criteria filtering
- Signal correlation tracking

### Ingestion Pipeline (`src/ingest/`)
Orchestrates the complete processing flow:
1. Signal normalization
2. Anomaly detection
3. Confidence scoring
4. Severity calculation
5. Alert generation and publishing

### Streaming Consumers (`src/streaming/`)
Support for real-time streaming ingestion:
- Kafka consumer
- NATS consumer
- Concurrent multi-consumer support

## Model Versioning

All components maintain version numbers for reproducibility:
- Signal Normalizer: v1.0.0
- Anomaly Detector: v1.0.0
- Confidence Engine: v1.0.0

Model weights are recorded in detection metadata for each alert.

## Development

Project structure:
```
.
├── src/
│   ├── api/          # FastAPI application
│   ├── models/       # Pydantic data models
│   ├── normalization/# Signal normalization
│   ├── detection/    # Anomaly detection
│   ├── confidence/   # Confidence scoring
│   ├── alerts/       # Alert publishing
│   ├── ingest/       # Pipeline orchestration
│   └── streaming/    # Kafka/NATS consumers
├── tests/            # Test suite
├── config/           # Configuration management
├── models/weights/   # Model weights (gitignored)
└── requirements.txt  # Dependencies
```

## License

See LICENSE file for details.
