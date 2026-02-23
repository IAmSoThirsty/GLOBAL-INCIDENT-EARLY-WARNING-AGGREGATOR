# Quick Start Guide

## Installation

```bash
# Clone the repository
git clone https://github.com/IAmSoThirsty/GLOBAL-INCIDENT-EARLY-WARNING-AGGREGATOR.git
cd GLOBAL-INCIDENT-EARLY-WARNING-AGGREGATOR

# Install dependencies
pip install -r requirements.txt

# For development
pip install -r requirements-dev.txt
```

## Running the System

### Option 1: HTTP API Server

Start the API server:

```bash
python run_server.py
```

The API will be available at `http://localhost:8000`.

Access interactive API documentation at `http://localhost:8000/docs`.

### Option 2: Run the Demo

See the system in action with example data:

```bash
python example_demo.py
```

## Basic Usage

### Ingest a Signal via API

```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_id": "sensor-001",
    "signal_type": "seismic",
    "timestamp": "2026-02-23T08:00:00Z",
    "payload": {
      "magnitude": 6.5,
      "depth_km": 10,
      "latitude": 35.6762,
      "longitude": 139.6503
    }
  }'
```

### Query Alerts

```bash
# Get all alerts
curl http://localhost:8000/alerts

# Get high-severity alerts
curl "http://localhost:8000/alerts?min_severity=4"

# Get alerts for a specific region
curl "http://localhost:8000/alerts?region=Pacific-Ring-of-Fire"

# Get high-confidence alerts
curl "http://localhost:8000/alerts?min_confidence=0.8"
```

### Check System Stats

```bash
curl http://localhost:8000/stats
```

## Using the Python API

```python
import asyncio
from datetime import datetime
from src.ingest import IngestionPipeline
from src.models import IngestRequest, SignalType

async def process_signal():
    # Initialize pipeline
    pipeline = IngestionPipeline()

    # Create a signal
    request = IngestRequest(
        source_id="sensor-001",
        signal_type=SignalType.SEISMIC,
        timestamp=datetime.utcnow(),
        payload={"magnitude": 7.2, "depth_km": 10}
    )

    # Process it
    alert = await pipeline.process_signal(request)

    if alert:
        print(f"Alert generated: {alert.incident_id}")
        print(f"Severity: {alert.severity}")
        print(f"Confidence: {alert.confidence}")
        print(f"Action: {alert.recommended_action}")

asyncio.run(process_signal())
```

## Signal Types and Payloads

### Seismic
```json
{
  "magnitude": 5.2,
  "depth_km": 10,
  "latitude": 35.6762,
  "longitude": 139.6503
}
```

### Epidemiological
```json
{
  "infection_rate": 0.15,
  "cases_per_100k": 250,
  "growth_rate": 15.5
}
```

### Climate
```json
{
  "temperature_anomaly_celsius": 3.5,
  "precipitation_anomaly_percent": 75,
  "wind_speed_kmh": 120
}
```

### Satellite
```json
{
  "ndvi_change": -0.4,
  "thermal_anomaly_kelvin": 15,
  "change_detection_score": 0.85
}
```

## Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_api.py

# Run with coverage
pytest --cov=src tests/
```

## Streaming Ingest (Advanced)

### Kafka Setup

1. Configure in `.env`:
```env
ENABLE_KAFKA=true
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=signals
```

2. Start the consumer:
```bash
python run_streaming.py
```

3. Publish messages to the `signals` topic in JSON format

### NATS Setup

1. Configure in `.env`:
```env
ENABLE_NATS=true
NATS_SERVERS=nats://localhost:4222
NATS_SUBJECT=signals.*
```

2. Start the consumer:
```bash
python run_streaming.py
```

3. Publish messages to NATS subjects matching the pattern

## Configuration

Create a `.env` file:

```env
# API Settings
API_HOST=0.0.0.0
API_PORT=8000

# Detection Parameters
ANOMALY_WINDOW_SIZE=100
ANOMALY_Z_THRESHOLD=3.0
CONFIDENCE_LEVEL=0.95

# Logging
LOG_LEVEL=INFO
```

## Next Steps

1. Explore the API documentation at `/docs` endpoint
2. Run the demo script to see the system in action
3. Review the full README.md for detailed architecture information
4. Check the test suite for usage examples
5. Integrate with your data sources (Kafka, NATS, or HTTP)

## Troubleshooting

**Issue**: Tests failing with "Pipeline not initialized"
- **Solution**: Tests use fixtures to initialize the pipeline automatically. Ensure pytest-asyncio is installed.

**Issue**: Import errors
- **Solution**: Ensure you're running from the repository root and all dependencies are installed.

**Issue**: Streaming consumers not receiving messages
- **Solution**: Check that Kafka/NATS services are running and configuration is correct.

## Support

For issues and questions, please refer to the main README.md or create an issue in the repository.
