"""
Progress Tracker Module
=======================
Real-time progress tracking for data migration operations.
"""

from __future__ import annotations
import sys
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional


@dataclass
class ProgressState:
    """Current state of a migration progress."""
    table_name: str
    phase: str  # 'fetch' | 'convert' | 'insert' | 'validate' | 'swap'
    total_rows: int = 0
    processed_rows: int = 0
    start_time: float = field(default_factory=time.time)
    phase_start_time: float = field(default_factory=time.time)

    @property
    def percent(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return min(100.0, (self.processed_rows / self.total_rows) * 100)

    @property
    def elapsed_secs(self) -> float:
        return time.time() - self.start_time

    @property
    def phase_elapsed_secs(self) -> float:
        return time.time() - self.phase_start_time

    @property
    def rows_per_sec(self) -> float:
        if self.phase_elapsed_secs == 0:
            return 0.0
        return self.processed_rows / self.phase_elapsed_secs

    @property
    def eta_secs(self) -> Optional[float]:
        if self.rows_per_sec == 0 or self.total_rows == 0:
            return None
        remaining = self.total_rows - self.processed_rows
        return remaining / self.rows_per_sec


class ProgressTracker:
    """
    Tracks and reports migration progress in real-time.
    
    Usage:
        tracker = ProgressTracker("my_table", total_rows=100000)
        tracker.start_phase("fetch")
        for batch in batches:
            tracker.update(len(batch))
        tracker.finish_phase()
        tracker.start_phase("insert")
        ...
        tracker.complete()
    """

    def __init__(
        self,
        table_name: str,
        total_rows: int = 0,
        callback: Optional[Callable[[ProgressState], None]] = None,
        show_progress_bar: bool = True,
        update_interval: float = 0.5,
    ):
        """
        Parameters
        ----------
        table_name : str
            Name of table being migrated
        total_rows : int
            Total rows expected (0 if unknown)
        callback : Callable, optional
            Function to call on progress updates (for API/webhook integration)
        show_progress_bar : bool
            Whether to show console progress bar
        update_interval : float
            Minimum seconds between progress bar updates (to avoid flickering)
        """
        self.state = ProgressState(
            table_name=table_name,
            total_rows=total_rows,
            phase="init",
        )
        self.callback = callback
        self.show_progress_bar = show_progress_bar
        self.update_interval = update_interval
        self._last_update = 0.0
        self._lock = threading.Lock()
        self._completed = False

    def set_total(self, total_rows: int):
        """Set total rows (can be called after initial fetch)."""
        with self._lock:
            self.state.total_rows = total_rows

    def start_phase(self, phase: str):
        """Start a new phase (fetch, convert, insert, validate, swap)."""
        with self._lock:
            self.state.phase = phase
            self.state.phase_start_time = time.time()
            self.state.processed_rows = 0
        self._notify()
        if self.show_progress_bar:
            self._print_phase_start(phase)

    def update(self, rows_processed: int):
        """Update progress with number of rows processed in this batch."""
        with self._lock:
            self.state.processed_rows += rows_processed
        
        # Throttle updates
        now = time.time()
        if now - self._last_update >= self.update_interval:
            self._last_update = now
            self._notify()
            if self.show_progress_bar:
                self._print_progress()

    def finish_phase(self):
        """Mark current phase as complete."""
        if self.show_progress_bar:
            self._print_phase_complete()
        self._notify()

    def complete(self, success: bool = True):
        """Mark migration as complete."""
        with self._lock:
            self._completed = True
            self.state.phase = "complete" if success else "failed"
        self._notify()
        if self.show_progress_bar:
            self._print_complete(success)

    def _notify(self):
        """Notify callback with current state."""
        if self.callback:
            try:
                self.callback(self.state)
            except Exception:
                pass  # Don't let callback errors break migration

    def _print_phase_start(self, phase: str):
        """Print phase start message."""
        phase_labels = {
            "fetch": "📥 Fetching from BigQuery",
            "convert": "🔄 Converting data",
            "insert": "📤 Inserting to PostgreSQL",
            "validate": "✅ Validating data",
            "swap": "⚡ Atomic table swap",
        }
        label = phase_labels.get(phase, f"🔧 {phase}")
        print(f"\n{label}...")

    def _print_progress(self):
        """Print progress bar to console."""
        state = self.state
        
        # Build progress bar
        bar_width = 30
        filled = int(bar_width * state.percent / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        
        # Format numbers
        processed = f"{state.processed_rows:,}"
        total = f"{state.total_rows:,}" if state.total_rows > 0 else "?"
        rate = f"{state.rows_per_sec:,.0f} rows/s" if state.rows_per_sec > 0 else ""
        
        # ETA
        eta = ""
        if state.eta_secs is not None and state.eta_secs > 0:
            eta_mins = int(state.eta_secs // 60)
            eta_secs = int(state.eta_secs % 60)
            eta = f"ETA {eta_mins}:{eta_secs:02d}"
        
        # Print (overwrite current line)
        line = f"\r   [{bar}] {state.percent:5.1f}% | {processed}/{total} | {rate} {eta}   "
        sys.stdout.write(line)
        sys.stdout.flush()

    def _print_phase_complete(self):
        """Print phase completion."""
        state = self.state
        duration = state.phase_elapsed_secs
        rows = state.processed_rows
        rate = rows / duration if duration > 0 else 0
        
        # Clear progress bar line and print summary
        sys.stdout.write("\r" + " " * 80 + "\r")
        print(f"   ✓ {rows:,} rows in {duration:.2f}s ({rate:,.0f} rows/s)")

    def _print_complete(self, success: bool):
        """Print final completion message."""
        total_time = self.state.elapsed_secs
        mins = int(total_time // 60)
        secs = total_time % 60
        
        if success:
            print(f"\n✅ Migration complete in {mins}m {secs:.1f}s")
        else:
            print(f"\n❌ Migration failed after {mins}m {secs:.1f}s")


def create_progress_callback_for_api(webhook_url: Optional[str] = None):
    """
    Create a progress callback that can post updates to a webhook.
    
    Returns a callback function that can be passed to ProgressTracker.
    """
    if not webhook_url:
        return None

    import requests

    def callback(state: ProgressState):
        try:
            requests.post(
                webhook_url,
                json={
                    "table": state.table_name,
                    "phase": state.phase,
                    "percent": state.percent,
                    "processed": state.processed_rows,
                    "total": state.total_rows,
                    "elapsed_secs": state.elapsed_secs,
                    "rows_per_sec": state.rows_per_sec,
                },
                timeout=2,
            )
        except Exception:
            pass  # Don't fail on webhook errors

    return callback
