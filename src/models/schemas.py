"""Data models for signals and alerts."""
from enum import Enum
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, validator
import uuid


class SignalType(str, Enum):
    """Types of signals supported by the system."""
    SEISMIC = "seismic"
    EPIDEMIOLOGICAL = "epidemiological"
    CLIMATE = "climate"
    SATELLITE = "satellite"


class IngestRequest(BaseModel):
    """Request model for signal ingestion."""
    source_id: str = Field(..., description="Identifier of the data source")
    signal_type: SignalType = Field(..., description="Type of signal being ingested")
    timestamp: datetime = Field(..., description="ISO8601 timestamp of the signal")
    payload: Dict[str, Any] = Field(..., description="Signal-specific data payload")

    class Config:
        json_schema_extra = {
            "example": {
                "source_id": "sensor-001",
                "signal_type": "seismic",
                "timestamp": "2026-02-23T08:00:00Z",
                "payload": {
                    "magnitude": 5.2,
                    "depth_km": 10,
                    "latitude": 35.6762,
                    "longitude": 139.6503
                }
            }
        }


class NormalizedSignal(BaseModel):
    """Normalized signal after processing."""
    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_id: str
    signal_type: SignalType
    timestamp: datetime
    normalized_value: float = Field(..., description="Normalized signal value [0-1]")
    raw_payload: Dict[str, Any]
    normalization_version: str = Field(default="1.0.0")


class ConfidenceBounds(BaseModel):
    """Confidence interval for an alert."""
    lower: float = Field(..., ge=0.0, le=1.0)
    upper: float = Field(..., ge=0.0, le=1.0)
    confidence_level: float = Field(default=0.95, ge=0.0, le=1.0)

    @validator('upper')
    def upper_must_be_greater(cls, v, values):
        if 'lower' in values and v < values['lower']:
            raise ValueError('upper bound must be >= lower bound')
        return v


class Alert(BaseModel):
    """Alert object emitted by the system."""
    incident_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    severity: int = Field(..., ge=1, le=5, description="Severity level 1-5")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score")
    confidence_bounds: ConfidenceBounds = Field(..., description="Confidence interval")
    region: str = Field(..., description="Geographic region identifier")
    signal_correlations: List[str] = Field(default_factory=list, description="Related signal IDs")
    recommended_action: str = Field(..., description="Recommended response action")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    model_version: str = Field(default="1.0.0", description="Detection model version")
    detection_metadata: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        json_schema_extra = {
            "example": {
                "incident_id": "550e8400-e29b-41d4-a716-446655440000",
                "severity": 4,
                "confidence": 0.87,
                "confidence_bounds": {
                    "lower": 0.82,
                    "upper": 0.92,
                    "confidence_level": 0.95
                },
                "region": "Pacific-Ring-of-Fire",
                "signal_correlations": ["sig-001", "sig-002"],
                "recommended_action": "Monitor seismic activity and prepare for potential aftershocks",
                "model_version": "1.0.0"
            }
        }


class AlertQueryParams(BaseModel):
    """Query parameters for alert retrieval."""
    region: Optional[str] = None
    min_severity: Optional[int] = Field(None, ge=1, le=5)
    min_confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = Field(default=100, ge=1, le=1000)
