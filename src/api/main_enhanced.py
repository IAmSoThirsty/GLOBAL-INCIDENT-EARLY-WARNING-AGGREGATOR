"""Enhanced FastAPI application with production-grade features."""
from typing import List, Optional
from datetime import datetime
from contextlib import asynccontextmanager
import psutil
import logging

from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from src.models import IngestRequest, Alert, AlertQueryParams
from src.ingest import IngestionPipeline
from src.ingest.deduplication import EventDeduplicator
from src.ingest.circuit_breaker import CircuitBreaker, CircuitBreakerError
from src.security import get_api_key_manager, get_rate_limiter, api_key_header
from src.observability import MetricsCollector, model_weights_hash
from src.models.weights import get_model_registry
from src.persistence import PostgreSQLAlertStore
from config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global instances
pipeline: Optional[IngestionPipeline] = None
deduplicator: Optional[EventDeduplicator] = None
circuit_breaker: Optional[CircuitBreaker] = None
alert_store: Optional[PostgreSQLAlertStore] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global pipeline, deduplicator, circuit_breaker, alert_store

    logger.info("Initializing GIEWA...")

    # Initialize model registry and record hash
    model_registry = get_model_registry()
    model_weights_hash.info({'hash': model_registry.get_hash()})
    logger.info(f"Model weights hash: {model_registry.get_hash()}")

    # Initialize event deduplicator
    deduplicator = EventDeduplicator(
        window_seconds=settings.dedup_window_seconds,
        max_events=settings.max_tracked_events
    )
    logger.info("Event deduplicator initialized")

    # Initialize circuit breaker
    if settings.circuit_breaker_enabled:
        circuit_breaker = CircuitBreaker(
            failure_threshold=settings.circuit_failure_threshold,
            recovery_timeout=settings.circuit_recovery_timeout,
            queue_depth_threshold=settings.queue_depth_threshold,
            memory_threshold_mb=settings.memory_threshold_mb
        )
        logger.info("Circuit breaker initialized")

    # Initialize PostgreSQL if enabled
    if settings.enable_postgres:
        alert_store = PostgreSQLAlertStore(settings.database_url)
        await alert_store.initialize()
        logger.info("PostgreSQL alert store initialized")

    # Initialize ingestion pipeline
    pipeline = IngestionPipeline()
    logger.info("Pipeline initialized successfully")

    yield

    # Cleanup
    logger.info("Shutting down...")
    if alert_store:
        await alert_store.close()


app = FastAPI(
    title="Global Incident Early Warning Aggregator",
    description="Production-grade incident detection with deterministic reproducibility",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request size limiting middleware
@app.middleware("http")
async def limit_request_size(request: Request, call_next):
    """Limit request body size."""
    content_length = request.headers.get("content-length")
    if content_length:
        if int(content_length) > settings.request_size_limit_mb * 1024 * 1024:
            return JSONResponse(
                status_code=413,
                content={"detail": "Request body too large"}
            )
    return await call_next(request)


async def get_api_key(api_key: Optional[str] = Depends(api_key_header)) -> Optional[str]:
    """Dependency for API key authentication."""
    if not settings.enable_auth:
        return None
    return api_key


@app.get("/")
async def root():
    """Root endpoint with system information."""
    model_registry = get_model_registry()
    return {
        "name": "Global Incident Early Warning Aggregator",
        "version": "1.0.0",
        "model_version": model_registry.get_version(),
        "model_hash": model_registry.get_hash(),
        "status": "operational",
        "features": {
            "authentication": settings.enable_auth,
            "rate_limiting": settings.enable_rate_limiting,
            "persistence": settings.enable_postgres,
            "circuit_breaker": settings.circuit_breaker_enabled,
            "metrics": settings.enable_metrics,
        },
        "endpoints": {
            "ingest": "POST /ingest",
            "alerts": "GET /alerts",
            "stats": "GET /stats",
            "metrics": "GET /metrics",
        }
    }


@app.post("/ingest", response_model=dict, status_code=202)
async def ingest_signal(
    request: IngestRequest,
    api_key: Optional[str] = Depends(get_api_key)
):
    """
    Ingest a signal with idempotent processing.

    Features:
    - Event deduplication for exactly-once semantics
    - Circuit breaker protection
    - Rate limiting
    - Prometheus metrics
    """
    if pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")

    # Validate API key and check permissions
    if settings.enable_auth:
        api_key_manager = get_api_key_manager()
        source_id = api_key_manager.validate_key(api_key, str(request.signal_type))
    else:
        source_id = request.source_id

    # Rate limiting
    if settings.enable_rate_limiting:
        rate_limiter = get_rate_limiter()
        rate_limiter.check_rate_limit(source_id)

    # Check circuit breaker
    if circuit_breaker and circuit_breaker.is_open():
        MetricsCollector.record_backpressure()
        raise HTTPException(
            status_code=503,
            detail="Service temporarily unavailable - circuit breaker open"
        )

    # Check memory and queue depth
    if circuit_breaker:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        queue_depth = 0  # Would track actual queue depth in production

        if circuit_breaker.check_overload(queue_depth, memory_mb):
            MetricsCollector.record_backpressure()
            raise HTTPException(
                status_code=503,
                detail="Service overloaded - please retry later"
            )

    try:
        # Event deduplication
        event_id = deduplicator.compute_event_id(
            request.source_id,
            request.timestamp.isoformat(),
            request.payload
        )

        if deduplicator.is_duplicate(event_id):
            MetricsCollector.record_duplicate(request.source_id)
            return {
                "status": "duplicate",
                "event_id": event_id,
                "message": "Event already processed (idempotent)",
            }

        # Record metrics
        MetricsCollector.record_ingest("http", str(request.signal_type))

        logger.info(
            f"Ingesting signal from {request.source_id} "
            f"(type: {request.signal_type}, event_id: {event_id[:16]}...)"
        )

        # Process signal through pipeline with timing
        with MetricsCollector.time_signal_processing(str(request.signal_type)):
            alert = await pipeline.process_signal(request)

        # Store alert if persistence is enabled
        if alert and alert_store:
            await alert_store.store_alert(alert)

        if alert:
            MetricsCollector.record_alert(alert.severity, alert.confidence)
            return {
                "status": "processed",
                "event_id": event_id,
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
                "event_id": event_id,
                "anomaly_detected": False,
                "alert_generated": False,
                "message": "Signal processed successfully",
            }

    except CircuitBreakerError as e:
        MetricsCollector.record_ingest_failure("circuit_breaker")
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        MetricsCollector.record_ingest_failure("validation")
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        MetricsCollector.record_ingest_failure("internal")
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
    """Query alerts based on filter criteria."""
    params = AlertQueryParams(
        region=region,
        min_severity=min_severity,
        min_confidence=min_confidence,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )

    # Use PostgreSQL if available, otherwise in-memory
    if alert_store:
        alerts = await alert_store.query_alerts(params)
    elif pipeline:
        alerts = pipeline.alert_publisher.query_alerts(params)
    else:
        raise HTTPException(status_code=503, detail="Service not available")

    logger.info(f"Retrieved {len(alerts)} alerts matching criteria")
    return alerts


@app.get("/alerts/{incident_id}", response_model=Alert)
async def get_alert_by_id(incident_id: str):
    """Retrieve a specific alert by incident ID."""
    if alert_store:
        alert = await alert_store.get_alert_by_id(incident_id)
    elif pipeline:
        alert = pipeline.alert_publisher.get_alert_by_id(incident_id)
    else:
        raise HTTPException(status_code=503, detail="Service not available")

    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")

    return alert


@app.get("/stats")
async def get_stats():
    """Get system statistics with operational metrics."""
    if pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")

    stats = {
        "pipeline": pipeline.get_stats(),
        "model": {
            "version": get_model_registry().get_version(),
            "hash": get_model_registry().get_hash(),
        },
    }

    if alert_store:
        stats["alerts"] = await alert_store.get_stats()
    else:
        stats["alerts"] = pipeline.alert_publisher.get_stats()

    if deduplicator:
        stats["deduplication"] = deduplicator.get_stats()

    if circuit_breaker:
        stats["circuit_breaker"] = circuit_breaker.get_stats()

    if settings.enable_rate_limiting:
        stats["rate_limiting"] = get_rate_limiter().get_stats()

    # System metrics
    process = psutil.Process()
    stats["system"] = {
        "memory_mb": process.memory_info().rss / 1024 / 1024,
        "cpu_percent": process.cpu_percent(),
    }

    return stats


@app.get("/health")
async def health_check():
    """Health check endpoint for Kubernetes probes."""
    if pipeline is None:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "reason": "Pipeline not initialized"}
        )

    # Check circuit breaker
    if circuit_breaker and circuit_breaker.is_open():
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "reason": "Circuit breaker open"}
        )

    # Check database if enabled
    if alert_store:
        try:
            await alert_store.get_stats()
        except Exception as e:
            return JSONResponse(
                status_code=503,
                content={"status": "unhealthy", "reason": f"Database error: {str(e)}"}
            )

    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    if not settings.enable_metrics:
        raise HTTPException(status_code=404, detail="Metrics not enabled")

    # Update storage metrics
    if pipeline:
        alert_stats = pipeline.alert_publisher.get_stats()
        MetricsCollector.update_storage_metrics(
            alert_stats.get("total_alerts", 0),
            settings.max_alerts
        )

    # Update deduplication metrics
    if deduplicator:
        from src.observability.metrics import deduplication_cache_size
        dedup_stats = deduplicator.get_stats()
        deduplication_cache_size.set(dedup_stats["tracked_events"])

    # Update circuit breaker state
    if circuit_breaker:
        MetricsCollector.set_circuit_breaker_state(circuit_breaker.get_state_value())

    # Update memory usage
    from src.observability.metrics import memory_usage_bytes
    process = psutil.Process()
    memory_usage_bytes.set(process.memory_info().rss)

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
