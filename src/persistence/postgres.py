"""PostgreSQL persistence layer for alerts."""
import asyncpg
from typing import List, Optional
from datetime import datetime
import json
import logging

from src.models import Alert, AlertQueryParams

logger = logging.getLogger(__name__)


class PostgreSQLAlertStore:
    """
    Durable alert storage with PostgreSQL.
    Provides indexed queries and persistence across restarts.
    """

    def __init__(self, connection_string: str):
        """
        Initialize PostgreSQL alert store.

        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self._pool: Optional[asyncpg.Pool] = None

    async def initialize(self):
        """Initialize database connection and create tables."""
        try:
            self._pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=2,
                max_size=10,
                command_timeout=60
            )

            # Create tables with indexes
            async with self._pool.acquire() as conn:
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS alerts (
                        incident_id VARCHAR(255) PRIMARY KEY,
                        severity INTEGER NOT NULL,
                        confidence REAL NOT NULL,
                        confidence_lower REAL NOT NULL,
                        confidence_upper REAL NOT NULL,
                        confidence_level REAL NOT NULL,
                        region VARCHAR(255) NOT NULL,
                        signal_correlations JSONB,
                        recommended_action TEXT NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        model_version VARCHAR(50) NOT NULL,
                        detection_metadata JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Create indexes for common queries
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_alerts_timestamp
                    ON alerts(timestamp DESC)
                ''')
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_alerts_region
                    ON alerts(region)
                ''')
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_alerts_severity
                    ON alerts(severity)
                ''')
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_alerts_confidence
                    ON alerts(confidence)
                ''')

            logger.info("PostgreSQL alert store initialized")

        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL: {e}")
            raise

    async def store_alert(self, alert: Alert) -> str:
        """
        Store alert in database.

        Args:
            alert: Alert to store

        Returns:
            incident_id
        """
        if not self._pool:
            raise RuntimeError("Database not initialized")

        async with self._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO alerts (
                    incident_id, severity, confidence,
                    confidence_lower, confidence_upper, confidence_level,
                    region, signal_correlations, recommended_action,
                    timestamp, model_version, detection_metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (incident_id) DO NOTHING
            ''',
                alert.incident_id,
                alert.severity,
                alert.confidence,
                alert.confidence_bounds.lower,
                alert.confidence_bounds.upper,
                alert.confidence_bounds.confidence_level,
                alert.region,
                json.dumps(alert.signal_correlations),
                alert.recommended_action,
                alert.timestamp,
                alert.model_version,
                json.dumps(alert.detection_metadata)
            )

        return alert.incident_id

    async def query_alerts(self, params: AlertQueryParams) -> List[Alert]:
        """
        Query alerts with filters.

        Args:
            params: Query parameters

        Returns:
            List of matching alerts
        """
        if not self._pool:
            raise RuntimeError("Database not initialized")

        # Build query
        conditions = ["1=1"]
        args = []
        arg_idx = 1

        if params.region:
            conditions.append(f"region = ${arg_idx}")
            args.append(params.region)
            arg_idx += 1

        if params.min_severity:
            conditions.append(f"severity >= ${arg_idx}")
            args.append(params.min_severity)
            arg_idx += 1

        if params.min_confidence:
            conditions.append(f"confidence >= ${arg_idx}")
            args.append(params.min_confidence)
            arg_idx += 1

        if params.start_time:
            conditions.append(f"timestamp >= ${arg_idx}")
            args.append(params.start_time)
            arg_idx += 1

        if params.end_time:
            conditions.append(f"timestamp <= ${arg_idx}")
            args.append(params.end_time)
            arg_idx += 1

        where_clause = " AND ".join(conditions)
        query = f'''
            SELECT * FROM alerts
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ${arg_idx}
        '''
        args.append(params.limit)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        # Convert rows to Alert objects
        alerts = []
        for row in rows:
            from src.models import ConfidenceBounds
            alert = Alert(
                incident_id=row['incident_id'],
                severity=row['severity'],
                confidence=row['confidence'],
                confidence_bounds=ConfidenceBounds(
                    lower=row['confidence_lower'],
                    upper=row['confidence_upper'],
                    confidence_level=row['confidence_level']
                ),
                region=row['region'],
                signal_correlations=json.loads(row['signal_correlations']),
                recommended_action=row['recommended_action'],
                timestamp=row['timestamp'],
                model_version=row['model_version'],
                detection_metadata=json.loads(row['detection_metadata'])
            )
            alerts.append(alert)

        return alerts

    async def get_alert_by_id(self, incident_id: str) -> Optional[Alert]:
        """
        Get alert by incident ID.

        Args:
            incident_id: Incident identifier

        Returns:
            Alert if found, None otherwise
        """
        if not self._pool:
            raise RuntimeError("Database not initialized")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT * FROM alerts WHERE incident_id = $1',
                incident_id
            )

        if not row:
            return None

        from src.models import ConfidenceBounds
        return Alert(
            incident_id=row['incident_id'],
            severity=row['severity'],
            confidence=row['confidence'],
            confidence_bounds=ConfidenceBounds(
                lower=row['confidence_lower'],
                upper=row['confidence_upper'],
                confidence_level=row['confidence_level']
            ),
            region=row['region'],
            signal_correlations=json.loads(row['signal_correlations']),
            recommended_action=row['recommended_action'],
            timestamp=row['timestamp'],
            model_version=row['model_version'],
            detection_metadata=json.loads(row['detection_metadata'])
        )

    async def get_stats(self) -> dict:
        """Get storage statistics."""
        if not self._pool:
            raise RuntimeError("Database not initialized")

        async with self._pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM alerts')
            avg_severity = await conn.fetchval('SELECT AVG(severity) FROM alerts')
            avg_confidence = await conn.fetchval('SELECT AVG(confidence) FROM alerts')

        return {
            "total_alerts": total,
            "avg_severity": float(avg_severity) if avg_severity else 0,
            "avg_confidence": float(avg_confidence) if avg_confidence else 0,
        }

    async def close(self):
        """Close database connections."""
        if self._pool:
            await self._pool.close()
            logger.info("PostgreSQL connections closed")
