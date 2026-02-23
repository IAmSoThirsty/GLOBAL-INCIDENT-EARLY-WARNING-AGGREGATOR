#!/usr/bin/env python3
"""Example demonstrating the complete ingestion pipeline."""
import asyncio
from datetime import datetime

from src.ingest import IngestionPipeline
from src.models import IngestRequest, SignalType


async def main():
    """Run example ingestion scenarios."""
    print("=" * 60)
    print("Global Incident Early Warning Aggregator - Demo")
    print("=" * 60)

    # Initialize pipeline
    pipeline = IngestionPipeline()

    print("\n1. Processing seismic signals...")
    print("-" * 60)

    # Feed normal seismic data
    for i in range(10):
        request = IngestRequest(
            source_id="sensor-pacific-001",
            signal_type=SignalType.SEISMIC,
            timestamp=datetime.utcnow(),
            payload={"magnitude": 3.0 + (i % 3) * 0.1, "depth_km": 10}
        )
        result = await pipeline.process_signal(request)
        if result:
            print(f"  ⚠️  Alert generated: Severity {result.severity}, "
                  f"Confidence {result.confidence:.2f}")

    # Feed anomalous seismic data
    print("\n  Injecting high-magnitude earthquake signal...")
    anomalous_request = IngestRequest(
        source_id="sensor-pacific-001",
        signal_type=SignalType.SEISMIC,
        timestamp=datetime.utcnow(),
        payload={"magnitude": 7.8, "depth_km": 10, "latitude": 35.6, "longitude": 139.7}
    )
    result = await pipeline.process_signal(anomalous_request)
    if result:
        print(f"  🚨 ALERT: {result.incident_id}")
        print(f"     Severity: {result.severity}/5")
        print(f"     Confidence: {result.confidence:.2f} "
              f"({result.confidence_bounds.lower:.2f}-{result.confidence_bounds.upper:.2f})")
        print(f"     Region: {result.region}")
        print(f"     Action: {result.recommended_action}")

    print("\n2. Processing epidemiological signals...")
    print("-" * 60)

    # Feed normal epidemiological data
    for i in range(10):
        request = IngestRequest(
            source_id="health-dept-001",
            signal_type=SignalType.EPIDEMIOLOGICAL,
            timestamp=datetime.utcnow(),
            payload={
                "infection_rate": 0.05,
                "cases_per_100k": 50,
                "growth_rate": 2.0
            }
        )
        await pipeline.process_signal(request)

    # Feed anomalous epidemiological data
    print("  Injecting outbreak signal...")
    outbreak_request = IngestRequest(
        source_id="health-dept-001",
        signal_type=SignalType.EPIDEMIOLOGICAL,
        timestamp=datetime.utcnow(),
        payload={
            "infection_rate": 0.65,
            "cases_per_100k": 850,
            "growth_rate": 35.0
        }
    )
    result = await pipeline.process_signal(outbreak_request)
    if result:
        print(f"  🚨 ALERT: {result.incident_id}")
        print(f"     Severity: {result.severity}/5")
        print(f"     Confidence: {result.confidence:.2f}")
        print(f"     Action: {result.recommended_action}")

    print("\n3. System Statistics")
    print("-" * 60)
    stats = pipeline.get_stats()
    print(f"  Signals processed: {stats['signals_processed']}")
    print(f"  Anomalies detected: {stats['anomalies_detected']}")
    print(f"  Alerts generated: {stats['alerts_generated']}")
    print(f"  Detection rate: {stats['detection_rate']:.1%}")

    alert_stats = pipeline.alert_publisher.get_stats()
    print(f"\n  Alert statistics:")
    print(f"    Total alerts: {alert_stats['total_alerts']}")
    if alert_stats['total_alerts'] > 0:
        print(f"    Average severity: {alert_stats['avg_severity']:.1f}/5")
        print(f"    Average confidence: {alert_stats['avg_confidence']:.2f}")

    print("\n4. Querying recent alerts...")
    print("-" * 60)
    from src.models import AlertQueryParams

    # Query high-severity alerts
    alerts = pipeline.alert_publisher.query_alerts(
        AlertQueryParams(min_severity=3, limit=5)
    )
    print(f"  Found {len(alerts)} high-severity alerts:")
    for alert in alerts:
        print(f"    - {alert.incident_id[:8]}... | "
              f"Severity {alert.severity} | "
              f"Confidence {alert.confidence:.2f} | "
              f"Region: {alert.region}")

    print("\n" + "=" * 60)
    print("Demo completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
