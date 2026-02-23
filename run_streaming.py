#!/usr/bin/env python3
"""Run streaming consumers."""
import asyncio
import logging

from config import settings
from src.ingest import IngestionPipeline
from src.streaming import StreamingManager

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point for streaming consumers."""
    logger.info("Initializing streaming consumers...")

    # Create pipeline
    pipeline = IngestionPipeline()

    # Create streaming manager
    manager = StreamingManager(pipeline)

    # Add configured consumers
    if settings.enable_kafka:
        logger.info("Configuring Kafka consumer...")
        manager.add_kafka_consumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            group_id=settings.kafka_group_id,
        )

    if settings.enable_nats:
        logger.info("Configuring NATS consumer...")
        manager.add_nats_consumer(
            servers=settings.nats_servers,
            subject=settings.nats_subject,
        )

    if not settings.enable_kafka and not settings.enable_nats:
        logger.warning(
            "No streaming consumers enabled. "
            "Set enable_kafka=True or enable_nats=True in configuration."
        )
        return

    # Start all consumers
    try:
        await manager.start_all()
        # Keep running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
    finally:
        await manager.stop_all()


if __name__ == "__main__":
    asyncio.run(main())
