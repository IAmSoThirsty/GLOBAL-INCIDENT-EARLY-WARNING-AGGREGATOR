"""Configuration management."""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings."""

    # API Settings
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_reload: bool = False
    api_secret_key: str = "CHANGE_ME_IN_PRODUCTION"

    # Security Settings
    enable_auth: bool = False
    enable_rate_limiting: bool = True
    global_rate_limit: int = 1000
    per_source_rate_limit: int = 100
    rate_limit_window: int = 60
    enable_signature_validation: bool = False
    hmac_secret: str = "CHANGE_ME_IN_PRODUCTION"
    request_size_limit_mb: int = 10

    # Pipeline Settings
    anomaly_window_size: int = 100
    anomaly_z_threshold: float = 3.0
    confidence_level: float = 0.95
    max_alerts: int = 10000

    # Streaming Settings
    enable_kafka: bool = False
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "signals"
    kafka_group_id: str = "early-warning-aggregator"

    enable_nats: bool = False
    nats_servers: str = "nats://localhost:4222"
    nats_subject: str = "signals.*"

    # Persistence Settings
    enable_postgres: bool = False
    database_url: str = "postgresql://giewa:giewa@localhost:5432/giewa"

    # Deduplication Settings
    dedup_window_seconds: int = 3600
    max_tracked_events: int = 100000

    # Circuit Breaker Settings
    circuit_breaker_enabled: bool = True
    circuit_failure_threshold: int = 5
    circuit_recovery_timeout: float = 30.0
    queue_depth_threshold: int = 1000
    memory_threshold_mb: int = 1536

    # Observability Settings
    enable_metrics: bool = True
    metrics_port: int = 8000

    # Logging
    log_level: str = "INFO"

    # Model Determinism
    numpy_seed: int = 42

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
