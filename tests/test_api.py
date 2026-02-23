"""Test API endpoints."""
import pytest
import pytest_asyncio
from datetime import datetime
from httpx import AsyncClient, ASGITransport

from src.api.main import app
from src.models import SignalType
from src.ingest import IngestionPipeline


@pytest_asyncio.fixture(scope="function", autouse=True)
async def initialize_pipeline():
    """Initialize pipeline before each test."""
    # Import the app module to set the pipeline
    from src.api import main
    main.pipeline = IngestionPipeline()
    yield
    main.pipeline = None


@pytest.mark.asyncio
async def test_root_endpoint():
    """Test root endpoint."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/")

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Global Incident Early Warning Aggregator"
    assert "endpoints" in data


@pytest.mark.asyncio
async def test_health_check():
    """Test health check endpoint."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


@pytest.mark.asyncio
async def test_ingest_endpoint():
    """Test signal ingestion endpoint."""
    payload = {
        "source_id": "test-sensor-001",
        "signal_type": "seismic",
        "timestamp": datetime.utcnow().isoformat(),
        "payload": {
            "magnitude": 6.5,
            "depth_km": 10,
            "latitude": 35.6762,
            "longitude": 139.6503
        }
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/ingest", json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "processed"
    assert "anomaly_detected" in data


@pytest.mark.asyncio
async def test_get_alerts_endpoint():
    """Test alerts query endpoint."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # First, ingest some signals
        for i in range(5):
            payload = {
                "source_id": f"sensor-{i}",
                "signal_type": "seismic",
                "timestamp": datetime.utcnow().isoformat(),
                "payload": {"magnitude": 7.0 + i}
            }
            await client.post("/ingest", json=payload)

        # Query alerts
        response = await client.get("/alerts")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_get_alerts_with_filters():
    """Test alerts query with filters."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/alerts",
            params={"min_severity": 3, "limit": 10}
        )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_get_stats():
    """Test stats endpoint."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/stats")

    assert response.status_code == 200
    data = response.json()
    assert "pipeline" in data
    assert "alerts" in data
