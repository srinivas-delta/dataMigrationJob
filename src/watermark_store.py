"""
Watermark Store
===============
Manages two Postgres tables that track sync state:

  sync_watermarks  — one row per BQ table; stores last successful sync time + status
  sync_logs        — append-only history of every sync run (success or failure)

These tables are created automatically on first use (idempotent).
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text


_DDL_WATERMARKS = """
CREATE TABLE IF NOT EXISTS sync_watermarks (
    table_name      TEXT PRIMARY KEY,
    bq_table_ref    TEXT,
    last_synced_at  TIMESTAMPTZ,
    status          TEXT DEFAULT 'never_run',   -- never_run | running | success | failed
    last_row_count  INTEGER,
    last_error      TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
"""

_DDL_LOGS = """
CREATE TABLE IF NOT EXISTS sync_logs (
    id              BIGSERIAL PRIMARY KEY,
    table_name      TEXT NOT NULL,
    bq_table_ref    TEXT,
    status          TEXT NOT NULL,              -- success | failed | skipped
    rows_affected   INTEGER,
    columns_added   INTEGER DEFAULT 0,
    columns_dropped INTEGER DEFAULT 0,
    type_mismatches INTEGER DEFAULT 0,
    duration_secs   NUMERIC(10, 3),
    drift_summary   TEXT,
    error           TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_sync_logs_table  ON sync_logs (table_name);
CREATE INDEX IF NOT EXISTS idx_sync_logs_status ON sync_logs (status);
CREATE INDEX IF NOT EXISTS idx_sync_logs_started ON sync_logs (started_at DESC);
"""


class WatermarkStore:
    """
    Thin wrapper around sync_watermarks and sync_logs.

    Parameters
    ----------
    pg_engine : SQLAlchemy engine
    """

    def __init__(self, pg_engine):
        self.engine = pg_engine

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def ensure_tables(self):
        """Create watermark + log tables if they don't exist. Idempotent."""
        with self.engine.connect() as conn:
            conn.execute(text(_DDL_WATERMARKS))
            conn.execute(text(_DDL_LOGS))
            conn.commit()

    # ------------------------------------------------------------------
    # Watermark (per-table state)
    # ------------------------------------------------------------------

    def get_watermark(self, table_name: str) -> Optional[str]:
        """
        Returns the last successful sync timestamp as an ISO string,
        or None if the table has never been synced.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT last_synced_at FROM sync_watermarks
                    WHERE table_name = :tn AND last_synced_at IS NOT NULL
                """),
                {"tn": table_name},
            ).fetchone()

        if row and row[0]:
            # Return as ISO string without microseconds for BQ TIMESTAMP()
            ts: datetime = row[0]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts.strftime("%Y-%m-%dT%H:%M:%S")
        return None

    def set_running(self, table_name: str, bq_table_ref: str) -> None:
        """Mark a table sync as in-progress."""
        self._upsert_watermark(table_name, bq_table_ref, status="running")

    def set_success(
        self,
        table_name: str,
        bq_table_ref: str,
        synced_at: datetime,
        row_count: int,
    ) -> None:
        """Mark a table sync as succeeded and advance the watermark."""
        self._upsert_watermark(
            table_name,
            bq_table_ref,
            status="success",
            last_synced_at=synced_at,
            last_row_count=row_count,
            last_error=None,
        )

    def set_failed(self, table_name: str, bq_table_ref: str, error: str) -> None:
        """Mark a table sync as failed (watermark NOT advanced)."""
        self._upsert_watermark(
            table_name,
            bq_table_ref,
            status="failed",
            last_error=error[:500],
        )

    def get_all_status(self) -> List[Dict[str, Any]]:
        """Return current status for all known tables."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT table_name, bq_table_ref, last_synced_at,
                           status, last_row_count, last_error, updated_at
                    FROM sync_watermarks
                    ORDER BY table_name
                """)
            ).fetchall()

        return [
            {
                "table_name":    r[0],
                "bq_table_ref":  r[1],
                "last_synced_at": r[2].isoformat() if r[2] else None,
                "status":        r[3],
                "last_row_count": r[4],
                "last_error":    r[5],
                "updated_at":    r[6].isoformat() if r[6] else None,
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Logs (append-only history)
    # ------------------------------------------------------------------

    def log_start(self, table_name: str, bq_table_ref: str) -> int:
        """Insert a 'running' log row; return the log id."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    INSERT INTO sync_logs
                        (table_name, bq_table_ref, status, started_at)
                    VALUES (:tn, :bq, 'running', NOW())
                    RETURNING id
                """),
                {"tn": table_name, "bq": bq_table_ref},
            ).fetchone()
            conn.commit()
        return row[0]

    def log_finish(
        self,
        log_id: int,
        status: str,
        rows_affected: int = 0,
        duration_secs: float = 0.0,
        columns_added: int = 0,
        columns_dropped: int = 0,
        type_mismatches: int = 0,
        drift_summary: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """Update an existing log row with the final result."""
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE sync_logs SET
                        status          = :status,
                        rows_affected   = :rows,
                        duration_secs   = :dur,
                        columns_added   = :ca,
                        columns_dropped = :cd,
                        type_mismatches = :tm,
                        drift_summary   = :ds,
                        error           = :err,
                        finished_at     = NOW()
                    WHERE id = :id
                """),
                {
                    "status": status,
                    "rows":   rows_affected,
                    "dur":    round(duration_secs, 3),
                    "ca":     columns_added,
                    "cd":     columns_dropped,
                    "tm":     type_mismatches,
                    "ds":     drift_summary,
                    "err":    error[:500] if error else None,
                    "id":     log_id,
                },
            )
            conn.commit()

    def get_logs(
        self,
        limit: int = 50,
        table_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return recent sync logs, newest first."""
        where = "WHERE table_name = :tn" if table_name else ""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT id, table_name, bq_table_ref, status,
                           rows_affected, columns_added, columns_dropped,
                           type_mismatches, duration_secs,
                           drift_summary, error, started_at, finished_at
                    FROM sync_logs
                    {where}
                    ORDER BY started_at DESC
                    LIMIT :lim
                """),
                {"tn": table_name, "lim": limit},
            ).fetchall()

        return [
            {
                "id":              r[0],
                "table_name":      r[1],
                "bq_table_ref":    r[2],
                "status":          r[3],
                "rows_affected":   r[4],
                "columns_added":   r[5],
                "columns_dropped": r[6],
                "type_mismatches": r[7],
                "duration_secs":   float(r[8]) if r[8] else None,
                "drift_summary":   r[9],
                "error":           r[10],
                "started_at":      r[11].isoformat() if r[11] else None,
                "finished_at":     r[12].isoformat() if r[12] else None,
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _upsert_watermark(
        self,
        table_name: str,
        bq_table_ref: str,
        status: str,
        last_synced_at: Optional[datetime] = None,
        last_row_count: Optional[int] = None,
        last_error: Optional[str] = None,
    ) -> None:
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO sync_watermarks
                        (table_name, bq_table_ref, status, last_synced_at,
                         last_row_count, last_error, updated_at)
                    VALUES
                        (:tn, :bq, :status, :ts, :rc, :err, NOW())
                    ON CONFLICT (table_name) DO UPDATE SET
                        bq_table_ref   = EXCLUDED.bq_table_ref,
                        status         = EXCLUDED.status,
                        last_synced_at = COALESCE(EXCLUDED.last_synced_at,
                                                  sync_watermarks.last_synced_at),
                        last_row_count = COALESCE(EXCLUDED.last_row_count,
                                                  sync_watermarks.last_row_count),
                        last_error     = EXCLUDED.last_error,
                        updated_at     = NOW()
                """),
                {
                    "tn":     table_name,
                    "bq":     bq_table_ref,
                    "status": status,
                    "ts":     last_synced_at,
                    "rc":     last_row_count,
                    "err":    last_error,
                },
            )
            conn.commit()
