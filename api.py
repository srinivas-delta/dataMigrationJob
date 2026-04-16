"""
DataMigrationJob — FastAPI Sync API
=====================================
Endpoints:

  POST /api/sync                     Trigger sync for ALL tables in config
  POST /api/sync/{table_name}        Trigger sync for ONE table (short name, e.g. "accounts")
  GET  /api/sync/status              Per-table watermark + current status
  GET  /api/sync/logs?limit=50       Sync run history (newest first)
  GET  /health                       Liveness probe

Authentication
--------------
Set SYNC_API_KEY in .env.
Pass it as the  X-API-Key  header on every request.
If SYNC_API_KEY is not set, auth is disabled (development mode).

Run
---
  uvicorn api:app --host 0.0.0.0 --port 8080 --reload
  # or
  python api.py
"""

from __future__ import annotations

import asyncio
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine

from src.bq_to_postgres_migrator import BQToPostgresMigrator
from src.config_loader import ConfigLoader
from src.config_table import ConfigTableManager
from src.watermark_store import WatermarkStore
from src.logger import get_logger

log = get_logger("api")


# ---------------------------------------------------------------------------
# App-level state (populated in lifespan)
# ---------------------------------------------------------------------------

class AppState:
    pg_engine = None
    gcp_credentials = None
    watermark_store: Optional[WatermarkStore] = None
    migration_configs: List[Dict[str, Any]] = []
    # Short-name → full config mapping, e.g. {"accounts": {...}}
    table_index: Dict[str, Dict[str, Any]] = {}
    # Global batch_size from config (None = load all at once)
    batch_size: Optional[int] = None
    # Per-table lock to prevent concurrent syncs of the same table
    _locks: Dict[str, threading.Lock] = {}

    @classmethod
    def get_lock(cls, table_name: str) -> threading.Lock:
        if table_name not in cls._locks:
            cls._locks[table_name] = threading.Lock()
        return cls._locks[table_name]


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load credentials, build engine, ensure watermark tables exist."""
    log.info("API starting up...")

    gcp_creds = ConfigLoader.load_gcp_credentials()
    pg_cfg = ConfigLoader.load_postgres_config()
    pg_conn_str = ConfigLoader.build_postgres_connection_string(pg_cfg)

    engine = create_engine(
        pg_conn_str,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
    )

    store = WatermarkStore(engine)
    store.ensure_tables()  # CREATE IF NOT EXISTS — idempotent

    # Load tables config from database
    config_mgr = ConfigTableManager(pg_conn_str)
    config_mgr.ensure_table_exists()
    config = config_mgr.load_configs()
    migration_configs = config.get("migrations", [])
    global_batch_size = config.get("batch_size")  # None if not specified

    # Build short-name index
    table_index = {}
    for cfg in migration_configs:
        short = cfg["bq_table"].split(".")[-1].lower()
        table_index[short] = cfg

    AppState.pg_engine = engine
    AppState.gcp_credentials = gcp_creds
    AppState.watermark_store = store
    AppState.migration_configs = migration_configs
    AppState.table_index = table_index
    AppState.batch_size = global_batch_size

    log.info("API ready — %d table(s) configured.", len(migration_configs))
    yield
    log.info("API shutting down.")


app = FastAPI(
    title="DataMigrationJob Sync API",
    description="Triggers BigQuery → PostgreSQL incremental syncs with schema-drift handling.",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# API Key auth
# ---------------------------------------------------------------------------

_EXPECTED_KEY: Optional[str] = os.getenv("SYNC_API_KEY")

if not _EXPECTED_KEY:
    log.warning("SYNC_API_KEY not set — API key authentication is DISABLED (dev mode)")


async def require_api_key(request: Request):
    """Dependency: validate X-API-Key header."""
    if not _EXPECTED_KEY:
        return  # auth disabled
    key = request.headers.get("X-API-Key", "")
    if key != _EXPECTED_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header",
        )


# ---------------------------------------------------------------------------
# Background sync worker
# ---------------------------------------------------------------------------

def _run_sync(
    table_cfg: Dict[str, Any],
    force_full: bool = False,
) -> Dict[str, Any]:
    """
    Blocking sync worker — runs in a thread via BackgroundTasks.
    Returns result dict (also persisted to sync_logs / sync_watermarks).
    """
    store: WatermarkStore = AppState.watermark_store
    bq_table_ref = table_cfg["bq_table"]
    short_name = bq_table_ref.split(".")[-1].lower()
    mode = table_cfg.get("mode", "full")
    primary_key = table_cfg.get("primary_key", "id")
    watermark_col = table_cfg.get("watermark_column")
    drift_cfg = table_cfg.get("schema_drift", {})
    removed_policy = drift_cfg.get("removed_policy", "keep")
    # Use per-table batch_size if specified, else global, else None (load all at once)
    batch_size = table_cfg.get("batch_size", AppState.batch_size)

    lock = AppState.get_lock(short_name)

    # Skip if already running (non-blocking try)
    if not lock.acquire(blocking=False):
        log.warning("[%s] Already running — sync skipped.", short_name)
        return {"status": "skipped", "table": short_name, "reason": "already_running"}

    started_at = datetime.now(timezone.utc)
    log_id = store.log_start(short_name, bq_table_ref)
    store.set_running(short_name, bq_table_ref)

    try:
        # Build migrator
        pg_conn_str = ConfigLoader.build_postgres_connection_string(
            ConfigLoader.load_postgres_config()
        )
        migrator = BQToPostgresMigrator(
            bq_table_ref=bq_table_ref,
            gcp_credentials=AppState.gcp_credentials,
            pg_connection_string=pg_conn_str,
            primary_key=primary_key,
            removed_policy=removed_policy,
        )

        # Resolve watermark — unless forced full sync
        watermark_value = None
        if mode in ("safe_upsert", "watermark_append") and watermark_col and not force_full:
            watermark_value = store.get_watermark(short_name)

        # Run sync
        if mode == "safe_upsert" and not force_full:
            result = migrator.migrate_safe(
                batch_size=batch_size,
                watermark_col=watermark_col,
                watermark_value=watermark_value,
            )
        elif mode == "watermark_append" and not force_full:
            result = migrator.migrate_append(
                batch_size=batch_size,
                watermark_col=watermark_col,
                watermark_value=watermark_value,
            )
        elif mode == "atomic_swap":
            # Full reload with atomic table swap — no PK/watermark needed
            result = migrator.migrate_atomic_swap(batch_size=batch_size)
        else:
            result = migrator.migrate(batch_size=batch_size)

        # Advance watermark on success
        finished_at = datetime.now(timezone.utc)
        rows = result.get("rows_upserted", result.get("rows_transferred", 0))
        store.set_success(short_name, bq_table_ref, finished_at, rows)
        store.log_finish(
            log_id,
            status="success",
            rows_affected=rows,
            duration_secs=(finished_at - started_at).total_seconds(),
            columns_added=result.get("columns_added", 0),
            columns_dropped=result.get("columns_dropped_policy", 0),
            type_mismatches=result.get("type_mismatches", 0),
            drift_summary=result.get("drift_summary"),
        )
        log.info("[%s] SUCCESS | rows=%s | duration=%.2fs",
                 short_name, f"{rows:,}", (finished_at - started_at).total_seconds())
        return {"status": "success", "table": short_name, "rows": rows}

    except Exception as e:
        finished_at = datetime.now(timezone.utc)
        err_msg = str(e)
        store.set_failed(short_name, bq_table_ref, err_msg)
        store.log_finish(
            log_id,
            status="failed",
            duration_secs=(finished_at - started_at).total_seconds(),
            error=err_msg,
        )
        log.error("[%s] FAILED | error=%s", short_name, err_msg[:200])
        return {"status": "failed", "table": short_name, "error": err_msg[:300]}

    finally:
        lock.release()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
async def health():
    """Simple liveness probe — used by Cloud Scheduler and load balancers."""
    configured = len(AppState.migration_configs)
    return {"status": "ok", "tables_configured": configured}


@app.post(
    "/api/sync",
    summary="Sync all configured tables",
    tags=["sync"],
    dependencies=[Depends(require_api_key)],
)
async def sync_all(
    background_tasks: BackgroundTasks,
    force_full: bool = False,
):
    """
    Triggers an incremental (or full if `force_full=true`) sync for every table
    in `config/tables.json`.

    Returns immediately — sync runs in the background.
    Poll `/api/sync/status` or `/api/sync/logs` to track progress.

    Query params
    ------------
    force_full : bool — ignore watermark and do a full re-sync (default: false)
    """
    if not AppState.migration_configs:
        raise HTTPException(status_code=409, detail="No tables configured.")

    queued = []
    for cfg in AppState.migration_configs:
        short = cfg["bq_table"].split(".")[-1].lower()
        background_tasks.add_task(_run_sync, cfg, force_full)
        queued.append(short)

    return {
        "status": "accepted",
        "message": f"{len(queued)} table(s) queued for sync.",
        "tables": queued,
        "force_full": force_full,
        "track": "/api/sync/status",
    }


@app.post(
    "/api/sync/{table_name}",
    summary="Sync a single table by short name",
    tags=["sync"],
    dependencies=[Depends(require_api_key)],
)
async def sync_one(
    table_name: str,
    background_tasks: BackgroundTasks,
    force_full: bool = False,
):
    """
    Triggers sync for a single table. `table_name` is the short name —
    the last segment of the BQ reference (e.g. `accounts` for
    `project.dataset.accounts`).

    Query params
    ------------
    force_full : bool — ignore watermark and do a full re-sync (default: false)
    """
    short = table_name.lower()
    cfg = AppState.table_index.get(short)
    if not cfg:
        known = list(AppState.table_index.keys())
        raise HTTPException(
            status_code=404,
            detail=f"Table '{short}' not in config. Known tables: {known}",
        )

    background_tasks.add_task(_run_sync, cfg, force_full)

    return {
        "status": "accepted",
        "table": short,
        "bq_table": cfg["bq_table"],
        "mode": cfg.get("mode", "full"),
        "force_full": force_full,
        "track": f"/api/sync/logs?table={short}",
    }


@app.get(
    "/api/sync/status",
    summary="Current sync status for all tables",
    tags=["sync"],
    dependencies=[Depends(require_api_key)],
)
async def sync_status():
    """
    Returns the latest watermark and status for every table that has been synced.
    Uses the `sync_watermarks` table.
    """
    rows = AppState.watermark_store.get_all_status()
    # Enrich with config info (mode, watermark_col, removed_policy)
    index = AppState.table_index
    for row in rows:
        cfg = index.get(row["table_name"], {})
        row["mode"] = cfg.get("mode", "full")
        row["watermark_column"] = cfg.get("watermark_column")
        row["removed_policy"] = cfg.get("schema_drift", {}).get("removed_policy", "keep")

    return {"tables": rows, "count": len(rows)}


@app.get(
    "/api/sync/logs",
    summary="Sync run history",
    tags=["sync"],
    dependencies=[Depends(require_api_key)],
)
async def sync_logs(
    limit: int = 50,
    table: Optional[str] = None,
):
    """
    Returns recent sync run history from `sync_logs`, newest first.

    Query params
    ------------
    limit : int  — max rows to return (default 50)
    table : str  — filter by short table name (optional)
    """
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    logs = AppState.watermark_store.get_logs(
        limit=limit,
        table_name=table.lower() if table else None,
    )
    return {"logs": logs, "count": len(logs)}


# ---------------------------------------------------------------------------
# Dev entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
        reload=os.getenv("ENV", "production") == "development",
        log_level="info",
    )
