"""FastAPI application for Global Incident Early Warning Aggregator."""
from typing import List, Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import logging

from src.models import IngestRequest, Alert, AlertQueryParams
from src.ingest import IngestionPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global pipeline instance
pipeline: Optional[IngestionPipeline] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global pipeline
    logger.info("Initializing ingestion pipeline...")
    pipeline = IngestionPipeline()
    logger.info("Pipeline initialized successfully")
    yield
    logger.info("Shutting down...")


app = FastAPI(
    title="Global Incident Early Warning Aggregator",
    description="Aggregate multi-source telemetry and detect anomaly patterns",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """Root endpoint with system information."""
    return {
        "name": "Global Incident Early Warning Aggregator",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "ingest": "POST /ingest",
            "alerts": "GET /alerts",
            "stats": "GET /stats",
        }
    }


@app.post("/ingest", response_model=dict, status_code=202)
async def ingest_signal(request: IngestRequest):
    """
    Ingest a signal for processing.

    The signal will be normalized, analyzed for anomalies, and may generate an alert
    if anomalous patterns are detected.

    Args:
        request: Signal ingestion request

    Returns:
        Response with processing status and optional alert information
    """
    if pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")

    try:
        logger.info(
            f"Ingesting signal from {request.source_id} "
            f"(type: {request.signal_type})"
        )

        # Process signal through pipeline
        alert = await pipeline.process_signal(request)

        if alert:
            return {
                "status": "processed",
                "anomaly_detected": True,
                "alert_generated": True,
                "incident_id": alert.incident_id,
                "severity": alert.severity,
                "confidence": alert.confidence,
                "message": "Signal processed and alert generated",
            }
        else:
            return {
                "status": "processed",
                "anomaly_detected": False,
                "alert_generated": False,
                "message": "Signal processed successfully",
            }

    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error processing signal: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal processing error")


@app.get("/alerts", response_model=List[Alert])
async def get_alerts(
    region: Optional[str] = Query(None, description="Filter by region"),
    min_severity: Optional[int] = Query(None, ge=1, le=5, description="Minimum severity level"),
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0, description="Minimum confidence score"),
    start_time: Optional[datetime] = Query(None, description="Start time for alert range"),
    end_time: Optional[datetime] = Query(None, description="End time for alert range"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of alerts to return"),
):
    """
    Query alerts based on filter criteria.

    Returns alerts matching the specified filters, sorted by timestamp (most recent first).

    Args:
        region: Filter by geographic region
        min_severity: Minimum severity level (1-5)
        min_confidence: Minimum confidence score (0.0-1.0)
        start_time: Start of time range
        end_time: End of time range
        limit: Maximum number of results

    Returns:
        List of alerts matching the criteria
    """
    if pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")

    try:
        params = AlertQueryParams(
            region=region,
            min_severity=min_severity,
            min_confidence=min_confidence,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

        alerts = pipeline.alert_publisher.query_alerts(params)
        logger.info(f"Retrieved {len(alerts)} alerts matching criteria")

        return alerts

    except Exception as e:
        logger.error(f"Error querying alerts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error querying alerts")


@app.get("/alerts/{incident_id}", response_model=Alert)
async def get_alert_by_id(incident_id: str):
    """
    Retrieve a specific alert by incident ID.

    Args:
        incident_id: The unique incident identifier

    Returns:
        Alert object if found

    Raises:
        404 if alert not found
    """
    if pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")

    alert = pipeline.alert_publisher.get_alert_by_id(incident_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")

    return alert


@app.get("/stats")
async def get_stats():
    """
    Get system statistics.

    Returns:
        Dictionary with pipeline and alert statistics
    """
    if pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")

    pipeline_stats = pipeline.get_stats()
    alert_stats = pipeline.alert_publisher.get_stats()

    return {
        "pipeline": pipeline_stats,
        "alerts": alert_stats,
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    if pipeline is None:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "reason": "Pipeline not initialized"}
        )

    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
