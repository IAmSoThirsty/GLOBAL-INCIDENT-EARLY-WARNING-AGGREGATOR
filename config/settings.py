"""Configuration management."""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings."""

    # API Settings
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_reload: bool = False

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

    # Logging
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
