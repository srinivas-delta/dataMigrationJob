"""
App Logger
==========
Configures a single shared logger that writes to:
  1. Console (stdout) — human-readable, coloured level prefix
  2. logs/migration.log — rotating file, 10 MB per file, 7 files kept

Usage
-----
    from src.logger import get_logger
    log = get_logger(__name__)

    log.info("Starting sync for accounts")
    log.warning("Schema drift: column 'phone' dropped from BQ")
    log.error("Batch 4 failed: connection timeout")

Log file location
-----------------
  dataMigrationJob/logs/migration.log
  dataMigrationJob/logs/migration.log.1  (rolled over)
  ...
  dataMigrationJob/logs/migration.log.7
"""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

# File lives in <project_root>/logs/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_LOG_DIR      = _PROJECT_ROOT / "logs"
_LOG_FILE     = _LOG_DIR / "migration.log"

_MAX_BYTES    = 10 * 1024 * 1024   # 10 MB per file
_BACKUP_COUNT = 7                  # keep 7 rolled files (~70 MB total)

_FILE_FORMAT    = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_CONSOLE_FORMAT = "%(levelname)-8s %(message)s"
_DATE_FORMAT    = "%Y-%m-%d %H:%M:%S"

# ── Internal state ────────────────────────────────────────────────────────────

_configured = False


def _setup() -> None:
    global _configured
    if _configured:
        return

    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger("datamigration")
    root.setLevel(logging.DEBUG)

    # ── File handler (always DEBUG) ──
    fh = RotatingFileHandler(
        str(_LOG_FILE),
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(_FILE_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(fh)

    # ── Console handler (INFO and above) ──
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(_CONSOLE_FORMAT))
    root.addHandler(ch)

    # Silence noisy third-party libs
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str = "datamigration") -> logging.Logger:
    """
    Returns a logger under the 'datamigration' hierarchy.

    Example:
        log = get_logger(__name__)   # → datamigration.src.bq_to_postgres_migrator
        log = get_logger("api")      # → datamigration.api
    """
    _setup()
    # Prefix all loggers so they share the same handlers
    if not name.startswith("datamigration"):
        name = f"datamigration.{name}"
    return logging.getLogger(name)


# ── Structured run summary helpers ────────────────────────────────────────────

def log_run_summary(
    log: logging.Logger,
    results: list,
    triggered_by: str = "cli",
) -> None:
    """
    Write a structured summary block to the log after all tables have been processed.
    Covers: total tables, success count, failure count, total rows, total duration.
    Also logs per-table detail lines.

    Parameters
    ----------
    log          : logger instance
    results      : list of result dicts returned by migrate() / migrate_safe()
    triggered_by : 'cli' | 'api' | 'scheduler'
    """
    successful = [r for r in results if r.get("success")]
    failed     = [r for r in results if not r.get("success")]

    total_rows    = sum(r.get("rows_upserted", r.get("rows_transferred", 0)) for r in successful)
    total_secs    = sum(r.get("duration_seconds", 0) for r in successful)
    cols_added    = sum(r.get("columns_added", 0) for r in successful)
    cols_dropped  = sum(r.get("columns_dropped_policy", 0) for r in successful)
    type_drifts   = sum(r.get("type_mismatches", 0) for r in successful)

    sep = "=" * 60

    log.info(sep)
    log.info("SYNC RUN SUMMARY  [triggered_by=%s]", triggered_by)
    log.info(sep)
    log.info("  Tables total   : %d", len(results))
    log.info("  Tables success : %d", len(successful))
    log.info("  Tables failed  : %d", len(failed))
    log.info("  Rows processed : %s", f"{total_rows:,}")
    log.info("  Total duration : %.2fs", total_secs)

    if cols_added or cols_dropped or type_drifts:
        log.info("  Schema drift   : +%d cols added | -%d cols policy-handled | %d type warnings",
                 cols_added, cols_dropped, type_drifts)

    if successful:
        log.info("")
        log.info("  ✅ Successful tables:")
        for r in successful:
            rows     = r.get("rows_upserted", r.get("rows_transferred", 0))
            dur      = r.get("duration_seconds", 0)
            mode     = r.get("mode", "full")
            ca       = r.get("columns_added", 0)
            cd       = r.get("columns_dropped_policy", 0)
            drift    = f" [+{ca} cols, -{cd} policy]" if (ca or cd) else ""
            log.info("     %-40s %8s rows  %6.2fs  %s%s",
                     r.get("bq_table", "?"), f"{rows:,}", dur, mode, drift)

    if failed:
        log.info("")
        log.warning("  ❌ Failed tables:")
        for r in failed:
            log.warning("     %-40s ERROR: %s",
                        r.get("bq_table", "?"),
                        str(r.get("error", "unknown"))[:120])

    log.info(sep)
    log.info("Log file: %s", str(_LOG_FILE))
    log.info(sep)
