"""
Schema Drift Handler
====================
Detects and resolves schema differences between a BigQuery table (source of truth)
and the corresponding PostgreSQL table before each sync.

Drift scenarios handled:
  1. NEW column in BQ → ALTER TABLE ADD COLUMN (NULL-safe, live users unaffected)
  2. REMOVED column in BQ → Configurable: keep (default) | drop | nullify
  3. TYPE CHANGE → Logged as warning; auto-migration skipped (too risky for live data)
  4. TABLE MISSING in PG → Full DDL create (first-run or disaster recovery)
"""

from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum

from sqlalchemy import text
from google.cloud import bigquery

from .type_mappings import get_postgresql_type


class DriftAction(str, Enum):
    ADD_COLUMN = "ADD_COLUMN"          # Column exists in BQ, missing in PG
    DROP_COLUMN = "DROP_COLUMN"        # Column removed from BQ source
    TYPE_MISMATCH = "TYPE_MISMATCH"    # Column exists in both but types differ
    NO_CHANGE = "NO_CHANGE"            # Identical, nothing to do
    TABLE_MISSING = "TABLE_MISSING"    # PG table does not exist yet


@dataclass
class ColumnDrift:
    column_name: str
    action: DriftAction
    bq_type: Optional[str] = None        # BQ type (canonical)
    bq_pg_expected: Optional[str] = None # What PG type BQ maps to
    pg_current: Optional[str] = None     # What PG actually has
    applied: bool = False
    skipped: bool = False
    skip_reason: str = ""


@dataclass
class SchemaDriftReport:
    table_name: str
    table_existed: bool
    drifts: List[ColumnDrift] = field(default_factory=list)
    # ordered list of BQ columns to use in queries (after drift resolution)
    active_bq_columns: List[str] = field(default_factory=list)

    @property
    def added(self) -> List[ColumnDrift]:
        return [d for d in self.drifts if d.action == DriftAction.ADD_COLUMN]

    @property
    def dropped(self) -> List[ColumnDrift]:
        return [d for d in self.drifts if d.action == DriftAction.DROP_COLUMN]

    @property
    def type_mismatches(self) -> List[ColumnDrift]:
        return [d for d in self.drifts if d.action == DriftAction.TYPE_MISMATCH]

    @property
    def has_drift(self) -> bool:
        return bool(self.drifts)

    def summary(self) -> str:
        lines = [f"Schema drift report for '{self.table_name}':"]
        if not self.drifts:
            lines.append("  No drift detected — schemas are in sync.")
        else:
            for d in self.drifts:
                status = "APPLIED" if d.applied else ("SKIPPED" if d.skipped else "PENDING")
                lines.append(f"  [{d.action.value}] {d.column_name} — {status}"
                             + (f" ({d.skip_reason})" if d.skip_reason else ""))
        return "\n".join(lines)


class SchemaDriftHandler:
    """
    Compares BQ schema against PG and applies safe DDL changes.

    Parameters
    ----------
    pg_engine      : SQLAlchemy engine connected to Postgres
    pg_table_name  : Target Postgres table name (lower-cased)
    removed_policy : What to do when a BQ column disappears from source
                     "keep"    → Leave PG column as-is (safe default)
                     "nullify" → Set all values to NULL (marks stale data)
                     "drop"    → ALTER TABLE DROP COLUMN (destructive)
    """

    def __init__(
        self,
        pg_engine,
        pg_table_name: str,
        removed_policy: str = "keep",
    ):
        self.pg_engine = pg_engine
        self.pg_table_name = pg_table_name.lower()
        # Validate policy
        if removed_policy not in ("keep", "nullify", "drop"):
            raise ValueError(f"removed_policy must be 'keep', 'nullify', or 'drop'. Got: {removed_policy}")
        self.removed_policy = removed_policy

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def detect_and_apply(
        self,
        bq_schema: List[bigquery.SchemaField],
    ) -> SchemaDriftReport:
        """
        Main entry point. Call this before every sync.

        Returns a SchemaDriftReport which includes `active_bq_columns` —
        the ordered list of column names to use in the upsert query.
        """
        report = SchemaDriftReport(
            table_name=self.pg_table_name,
            table_existed=self._pg_table_exists(),
        )

        # Build lookup: column_name (lower) → SchemaField
        bq_cols: Dict[str, bigquery.SchemaField] = {
            f.name.lower(): f for f in bq_schema
        }

        if not report.table_existed:
            # Nothing to diff — caller will CREATE the table fresh
            drift = ColumnDrift(
                column_name="*",
                action=DriftAction.TABLE_MISSING,
                skipped=True,
                skip_reason="Table will be created by migrator",
            )
            report.drifts.append(drift)
            report.active_bq_columns = list(bq_cols.keys())
            return report

        # Fetch current PG schema
        pg_cols: Dict[str, str] = self._get_pg_columns()

        # --- 1. Columns in BQ but missing in PG → ADD --------------------
        for col_name, bq_field in bq_cols.items():
            if col_name not in pg_cols:
                pg_type = get_postgresql_type(bq_field.field_type, bq_field.mode)
                drift = ColumnDrift(
                    column_name=col_name,
                    action=DriftAction.ADD_COLUMN,
                    bq_type=bq_field.field_type,
                    bq_pg_expected=pg_type,
                )
                self._apply_add_column(col_name, pg_type, drift)
                report.drifts.append(drift)

        # --- 2. Columns in PG but missing in BQ → policy-driven ----------
        for col_name in pg_cols:
            if col_name not in bq_cols:
                drift = ColumnDrift(
                    column_name=col_name,
                    action=DriftAction.DROP_COLUMN,
                    pg_current=pg_cols[col_name],
                )
                self._apply_removed_column(col_name, drift)
                report.drifts.append(drift)

        # --- 3. Type mismatches (informational only) ---------------------
        for col_name, bq_field in bq_cols.items():
            if col_name in pg_cols:
                expected_pg = get_postgresql_type(bq_field.field_type, bq_field.mode)
                actual_pg = pg_cols[col_name].upper()
                # Normalise for loose comparison (e.g. "TEXT" vs "character varying")
                if not self._types_compatible(expected_pg, actual_pg):
                    drift = ColumnDrift(
                        column_name=col_name,
                        action=DriftAction.TYPE_MISMATCH,
                        bq_type=bq_field.field_type,
                        bq_pg_expected=expected_pg,
                        pg_current=actual_pg,
                        skipped=True,
                        skip_reason="Type changes require manual migration to avoid data loss",
                    )
                    report.drifts.append(drift)

        # active_bq_columns = all BQ columns (PG has been updated to match)
        report.active_bq_columns = [f.name.lower() for f in bq_schema]
        return report

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _pg_table_exists(self) -> bool:
        with self.pg_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name   = :tname
                LIMIT 1
            """), {"tname": self.pg_table_name})
            return result.fetchone() is not None

    def _get_pg_columns(self) -> Dict[str, str]:
        """Returns {column_name: data_type} from information_schema."""
        with self.pg_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name, udt_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name   = :tname
                ORDER BY ordinal_position
            """), {"tname": self.pg_table_name})
            rows = result.fetchall()

        # Use udt_name for user-defined types (e.g. jsonb, uuid),
        # fall back to data_type for standard types.
        cols = {}
        for row in rows:
            col_name = row[0].lower()
            udt = row[1].upper()       # e.g. INT4, TEXT, JSONB, TIMESTAMPTZ
            data_type = row[2].upper() # e.g. INTEGER, TEXT, USER-DEFINED
            # Prefer udt_name for richer info
            cols[col_name] = udt if udt else data_type
        return cols

    def _apply_add_column(self, col_name: str, pg_type: str, drift: ColumnDrift):
        """ADD COLUMN — always NULL-safe for live tables."""
        ddl = f'ALTER TABLE "{self.pg_table_name}" ADD COLUMN IF NOT EXISTS "{col_name}" {pg_type};'
        try:
            with self.pg_engine.connect() as conn:
                conn.execute(text(ddl))
                conn.commit()
            drift.applied = True
            print(f"  ✅ [DRIFT] ADD COLUMN  \"{col_name}\" {pg_type}")
        except Exception as e:
            drift.skipped = True
            drift.skip_reason = str(e)[:120]
            print(f"  ⚠️  [DRIFT] Failed to ADD COLUMN \"{col_name}\": {drift.skip_reason}")

    def _apply_removed_column(self, col_name: str, drift: ColumnDrift):
        """Handle a column that exists in PG but no longer in BQ."""
        if self.removed_policy == "keep":
            drift.skipped = True
            drift.skip_reason = "removed_policy=keep — column retained in PG"
            print(f"  ℹ️  [DRIFT] KEPT removed column \"{col_name}\" (removed from BQ source)")

        elif self.removed_policy == "nullify":
            sql = f'UPDATE "{self.pg_table_name}" SET "{col_name}" = NULL;'
            try:
                with self.pg_engine.connect() as conn:
                    conn.execute(text(sql))
                    conn.commit()
                drift.applied = True
                print(f"  ✅ [DRIFT] NULLIFIED column \"{col_name}\" (no longer in BQ)")
            except Exception as e:
                drift.skipped = True
                drift.skip_reason = str(e)[:120]
                print(f"  ⚠️  [DRIFT] Failed to nullify \"{col_name}\": {drift.skip_reason}")

        elif self.removed_policy == "drop":
            ddl = f'ALTER TABLE "{self.pg_table_name}" DROP COLUMN IF EXISTS "{col_name}";'
            try:
                with self.pg_engine.connect() as conn:
                    conn.execute(text(ddl))
                    conn.commit()
                drift.applied = True
                print(f"  ✅ [DRIFT] DROPPED column \"{col_name}\" (removed from BQ)")
            except Exception as e:
                drift.skipped = True
                drift.skip_reason = str(e)[:120]
                print(f"  ⚠️  [DRIFT] Failed to drop \"{col_name}\": {drift.skip_reason}")

    # ------------------------------------------------------------------
    # Type compatibility (loose matching)
    # ------------------------------------------------------------------
    # PG stores type names differently from what we declare in DDL.
    # e.g. BIGINT → INT8, TEXT → TEXT, TIMESTAMP WITH TIME ZONE → TIMESTAMPTZ
    _TYPE_ALIASES: Dict[str, List[str]] = {
        "BIGINT":                    ["INT8", "BIGINT"],
        "INTEGER":                   ["INT4", "INTEGER", "INT"],
        "SMALLINT":                  ["INT2", "SMALLINT"],
        "DOUBLE PRECISION":          ["FLOAT8", "DOUBLE PRECISION"],
        "REAL":                      ["FLOAT4", "REAL"],
        "NUMERIC":                   ["NUMERIC"],
        "TEXT":                      ["TEXT", "VARCHAR", "CHARACTER VARYING"],
        "BOOLEAN":                   ["BOOL", "BOOLEAN"],
        "BYTEA":                     ["BYTEA"],
        "JSONB":                     ["JSONB"],
        "DATE":                      ["DATE"],
        "TIME":                      ["TIME", "TIME WITHOUT TIME ZONE"],
        "TIMESTAMP":                 ["TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"],
        "TIMESTAMP WITH TIME ZONE":  ["TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"],
        "INTERVAL":                  ["INTERVAL"],
    }

    def _types_compatible(self, expected: str, actual: str) -> bool:
        expected_up = expected.upper().strip()
        actual_up = actual.upper().strip()

        if expected_up == actual_up:
            return True

        # Check alias groups
        for canonical, aliases in self._TYPE_ALIASES.items():
            aliases_up = [a.upper() for a in aliases]
            if expected_up in aliases_up and actual_up in aliases_up:
                return True

        # Partial match for parameterised types like NUMERIC(10,2)
        for alias in [expected_up, actual_up]:
            base = alias.split("(")[0].strip()
            for canonical, aliases in self._TYPE_ALIASES.items():
                if base in [a.upper() for a in aliases]:
                    return True

        return False
