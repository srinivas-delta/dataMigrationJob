"""
Parallel Sync Module
====================
Run multiple table migrations concurrently for faster bulk migrations.
"""

from __future__ import annotations
import concurrent.futures
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .bq_to_postgres_migrator import BQToPostgresMigrator


@dataclass
class ParallelSyncResult:
    """Result of a parallel sync operation."""
    table_name: str
    success: bool
    rows: int = 0
    duration_secs: float = 0.0
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


@dataclass
class ParallelSyncSummary:
    """Summary of all parallel sync operations."""
    total_tables: int
    successful: int
    failed: int
    total_rows: int
    total_duration_secs: float
    results: List[ParallelSyncResult] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [
            f"\n{'='*60}",
            f"Parallel Sync Summary",
            f"{'='*60}",
            f"  Tables: {self.successful}/{self.total_tables} successful",
            f"  Rows:   {self.total_rows:,}",
            f"  Time:   {self.total_duration_secs:.2f}s",
            f"{'='*60}",
        ]
        
        if self.failed > 0:
            lines.append("\nFailed tables:")
            for r in self.results:
                if not r.success:
                    lines.append(f"  ❌ {r.table_name}: {r.error}")
        
        return "\n".join(lines)


class ParallelSyncManager:
    """
    Manages parallel execution of multiple table migrations.
    
    Usage:
        manager = ParallelSyncManager(
            gcp_credentials=creds,
            pg_connection_string=pg_conn,
            max_workers=4,
        )
        
        tables = [
            {"bq_table": "proj.ds.table1", "mode": "atomic_swap"},
            {"bq_table": "proj.ds.table2", "mode": "safe_upsert", "primary_key": "id"},
        ]
        
        summary = manager.sync_all(tables, batch_size=1000)
        print(summary)
    """

    def __init__(
        self,
        gcp_credentials: Any,
        pg_connection_string: str,
        max_workers: int = 4,
        on_table_complete: Optional[Callable[[ParallelSyncResult], None]] = None,
    ):
        """
        Parameters
        ----------
        gcp_credentials : Any
            Google Cloud credentials
        pg_connection_string : str
            PostgreSQL connection string
        max_workers : int
            Maximum concurrent migrations (default: 4)
        on_table_complete : Callable, optional
            Callback when each table completes
        """
        self.gcp_credentials = gcp_credentials
        self.pg_connection_string = pg_connection_string
        self.max_workers = max_workers
        self.on_table_complete = on_table_complete
        self._lock = threading.Lock()
        self._results: List[ParallelSyncResult] = []

    def sync_all(
        self,
        table_configs: List[Dict[str, Any]],
        batch_size: Optional[int] = None,
        watermark_store: Optional[Any] = None,
    ) -> ParallelSyncSummary:
        """
        Run migrations for all tables in parallel.
        
        Parameters
        ----------
        table_configs : List[Dict]
            List of table configurations (same format as tables.json migrations)
        batch_size : int, optional
            Batch size for all tables (can be overridden per-table)
        watermark_store : WatermarkStore, optional
            For watermark-based modes
            
        Returns
        -------
        ParallelSyncSummary
            Summary of all sync operations
        """
        start_time = time.time()
        self._results = []
        
        print(f"\n{'='*60}")
        print(f"Starting parallel sync of {len(table_configs)} tables")
        print(f"Max workers: {self.max_workers}")
        print(f"{'='*60}\n")

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(
                    self._sync_table,
                    table_cfg,
                    batch_size,
                    watermark_store,
                ): table_cfg
                for table_cfg in table_configs
            }

            # Process completed futures
            for future in concurrent.futures.as_completed(futures):
                table_cfg = futures[future]
                table_name = table_cfg['bq_table'].split('.')[-1].lower()
                
                try:
                    result = future.result()
                    with self._lock:
                        self._results.append(result)
                    
                    status = "✅" if result.success else "❌"
                    print(f"{status} [{table_name}] {result.rows:,} rows in {result.duration_secs:.2f}s")
                    
                    if self.on_table_complete:
                        self.on_table_complete(result)
                        
                except Exception as e:
                    error_result = ParallelSyncResult(
                        table_name=table_name,
                        success=False,
                        error=str(e),
                    )
                    with self._lock:
                        self._results.append(error_result)
                    print(f"❌ [{table_name}] Failed: {str(e)[:100]}")

        total_duration = time.time() - start_time
        
        # Build summary
        successful = [r for r in self._results if r.success]
        failed = [r for r in self._results if not r.success]
        total_rows = sum(r.rows for r in successful)
        
        summary = ParallelSyncSummary(
            total_tables=len(table_configs),
            successful=len(successful),
            failed=len(failed),
            total_rows=total_rows,
            total_duration_secs=total_duration,
            results=self._results,
        )
        
        print(summary)
        return summary

    def _sync_table(
        self,
        table_cfg: Dict[str, Any],
        global_batch_size: Optional[int],
        watermark_store: Optional[Any],
    ) -> ParallelSyncResult:
        """
        Sync a single table (runs in a thread).
        """
        bq_table_ref = table_cfg['bq_table']
        short_name = bq_table_ref.split('.')[-1].lower()
        mode = table_cfg.get('mode', 'full')
        primary_key = table_cfg.get('primary_key', 'id')
        watermark_col = table_cfg.get('watermark_column')
        drift_cfg = table_cfg.get('schema_drift', {})
        removed_policy = drift_cfg.get('removed_policy', 'keep')
        batch_size = table_cfg.get('batch_size', global_batch_size)
        
        start_time = time.time()
        
        try:
            migrator = BQToPostgresMigrator(
                bq_table_ref=bq_table_ref,
                gcp_credentials=self.gcp_credentials,
                pg_connection_string=self.pg_connection_string,
                primary_key=primary_key,
                removed_policy=removed_policy,
                validate=True,
                show_progress=False,  # Disable progress bars in parallel mode
            )

            # Get watermark if needed
            watermark_value = None
            if watermark_store and mode in ('safe_upsert', 'watermark_append') and watermark_col:
                watermark_value = watermark_store.get_watermark(short_name)

            # Run appropriate migration mode
            if mode == 'safe_upsert':
                result = migrator.migrate_safe(
                    batch_size=batch_size,
                    watermark_col=watermark_col,
                    watermark_value=watermark_value,
                )
            elif mode == 'watermark_append':
                result = migrator.migrate_append(
                    batch_size=batch_size,
                    watermark_col=watermark_col,
                    watermark_value=watermark_value,
                )
            elif mode == 'atomic_swap':
                result = migrator.migrate_atomic_swap(batch_size=batch_size)
            else:
                result = migrator.migrate(batch_size=batch_size)

            duration = time.time() - start_time
            rows = result.get('rows_upserted', result.get('rows_transferred', 0))

            # Update watermark on success
            if watermark_store and result.get('success'):
                finished_at = datetime.now(timezone.utc)
                watermark_store.set_success(short_name, bq_table_ref, finished_at, rows)

            return ParallelSyncResult(
                table_name=short_name,
                success=result.get('success', False),
                rows=rows,
                duration_secs=duration,
                result=result,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ParallelSyncResult(
                table_name=short_name,
                success=False,
                duration_secs=duration,
                error=str(e),
            )


def sync_tables_parallel(
    table_configs: List[Dict[str, Any]],
    gcp_credentials: Any,
    pg_connection_string: str,
    max_workers: int = 4,
    batch_size: Optional[int] = None,
    watermark_store: Optional[Any] = None,
) -> ParallelSyncSummary:
    """
    Convenience function to run parallel sync without instantiating manager.
    
    Example:
        from src.parallel_sync import sync_tables_parallel
        
        summary = sync_tables_parallel(
            table_configs=config['migrations'],
            gcp_credentials=creds,
            pg_connection_string=pg_conn,
            max_workers=4,
        )
    """
    manager = ParallelSyncManager(
        gcp_credentials=gcp_credentials,
        pg_connection_string=pg_connection_string,
        max_workers=max_workers,
    )
    return manager.sync_all(
        table_configs=table_configs,
        batch_size=batch_size,
        watermark_store=watermark_store,
    )
