"""Streaming ingest support for Kafka and NATS."""
import asyncio
import logging
from typing import Optional, Callable, Awaitable
import json

from src.models import IngestRequest
from src.ingest import IngestionPipeline

logger = logging.getLogger(__name__)


class StreamingIngestBase:
    """Base class for streaming ingest consumers."""

    def __init__(self, pipeline: IngestionPipeline):
        """
        Initialize streaming consumer.

        Args:
            pipeline: Ingestion pipeline for processing signals
        """
        self.pipeline = pipeline
        self._running = False

    async def start(self):
        """Start consuming messages."""
        raise NotImplementedError

    async def stop(self):
        """Stop consuming messages."""
        self._running = False

    def _parse_message(self, message: bytes) -> Optional[IngestRequest]:
        """
        Parse message bytes into IngestRequest.

        Args:
            message: Raw message bytes

        Returns:
            IngestRequest if valid, None otherwise
        """
        try:
            data = json.loads(message.decode('utf-8'))
            return IngestRequest(**data)
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return None


class KafkaConsumer(StreamingIngestBase):
    """
    Kafka consumer for streaming signal ingestion.
    Requires aiokafka package.
    """

    def __init__(
        self,
        pipeline: IngestionPipeline,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "signals",
        group_id: str = "early-warning-aggregator",
    ):
        """
        Initialize Kafka consumer.

        Args:
            pipeline: Ingestion pipeline
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to consume from
            group_id: Consumer group ID
        """
        super().__init__(pipeline)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self._consumer = None

    async def start(self):
        """Start consuming from Kafka."""
        try:
            from aiokafka import AIOKafkaConsumer

            self._consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
            )

            await self._consumer.start()
            self._running = True
            logger.info(f"Kafka consumer started on topic '{self.topic}'")

            try:
                async for message in self._consumer:
                    if not self._running:
                        break

                    logger.debug(f"Received Kafka message: {message.value}")
                    request = self._parse_message(message.value)

                    if request:
                        try:
                            await self.pipeline.process_signal(request)
                        except Exception as e:
                            logger.error(f"Error processing signal: {e}")

            finally:
                await self._consumer.stop()

        except ImportError:
            logger.error(
                "aiokafka package not installed. "
                "Install with: pip install aiokafka"
            )
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}", exc_info=True)
            raise

    async def stop(self):
        """Stop Kafka consumer."""
        super().stop()
        if self._consumer:
            await self._consumer.stop()
        logger.info("Kafka consumer stopped")


class NATSConsumer(StreamingIngestBase):
    """
    NATS consumer for streaming signal ingestion.
    Requires asyncio-nats-client package.
    """

    def __init__(
        self,
        pipeline: IngestionPipeline,
        servers: str = "nats://localhost:4222",
        subject: str = "signals.*",
    ):
        """
        Initialize NATS consumer.

        Args:
            pipeline: Ingestion pipeline
            servers: NATS server URLs
            subject: Subject pattern to subscribe to
        """
        super().__init__(pipeline)
        self.servers = servers
        self.subject = subject
        self._client = None

    async def start(self):
        """Start consuming from NATS."""
        try:
            from nats.aio.client import Client as NATS

            self._client = NATS()
            await self._client.connect(servers=[self.servers])
            self._running = True
            logger.info(f"NATS consumer connected to {self.servers}")

            async def message_handler(msg):
                """Handle incoming NATS messages."""
                logger.debug(f"Received NATS message on {msg.subject}")
                request = self._parse_message(msg.data)

                if request:
                    try:
                        await self.pipeline.process_signal(request)
                    except Exception as e:
                        logger.error(f"Error processing signal: {e}")

            # Subscribe to subject
            await self._client.subscribe(self.subject, cb=message_handler)
            logger.info(f"NATS consumer subscribed to '{self.subject}'")

            # Keep running
            while self._running:
                await asyncio.sleep(1)

        except ImportError:
            logger.error(
                "asyncio-nats-client package not installed. "
                "Install with: pip install asyncio-nats-client"
            )
        except Exception as e:
            logger.error(f"NATS consumer error: {e}", exc_info=True)
            raise
        finally:
            if self._client:
                await self._client.close()

    async def stop(self):
        """Stop NATS consumer."""
        super().stop()
        if self._client:
            await self._client.close()
        logger.info("NATS consumer stopped")


class StreamingManager:
    """
    Manager for multiple streaming consumers.
    Supports running Kafka and NATS consumers simultaneously.
    """

    def __init__(self, pipeline: IngestionPipeline):
        """
        Initialize streaming manager.

        Args:
            pipeline: Ingestion pipeline
        """
        self.pipeline = pipeline
        self._consumers: list = []
        self._tasks: list = []

    def add_kafka_consumer(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "signals",
        group_id: str = "early-warning-aggregator",
    ):
        """Add a Kafka consumer."""
        consumer = KafkaConsumer(
            self.pipeline,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
        )
        self._consumers.append(consumer)
        logger.info(f"Added Kafka consumer for topic '{topic}'")

    def add_nats_consumer(
        self,
        servers: str = "nats://localhost:4222",
        subject: str = "signals.*",
    ):
        """Add a NATS consumer."""
        consumer = NATSConsumer(
            self.pipeline,
            servers=servers,
            subject=subject,
        )
        self._consumers.append(consumer)
        logger.info(f"Added NATS consumer for subject '{subject}'")

    async def start_all(self):
        """Start all configured consumers."""
        logger.info(f"Starting {len(self._consumers)} streaming consumers...")

        for consumer in self._consumers:
            task = asyncio.create_task(consumer.start())
            self._tasks.append(task)

        logger.info("All streaming consumers started")

    async def stop_all(self):
        """Stop all running consumers."""
        logger.info("Stopping all streaming consumers...")

        for consumer in self._consumers:
            await consumer.stop()

        # Wait for tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        logger.info("All streaming consumers stopped")
