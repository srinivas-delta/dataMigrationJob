"""
Data Validation Module
======================
Post-migration validation to ensure data integrity:
  - Row count validation: source rows == target rows
"""

from __future__ import annotations
import hashlib
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from google.cloud import bigquery
from sqlalchemy import text


@dataclass
class ValidationResult:
    """Result of a validation check."""
    passed: bool
    check_type: str  # 'row_count'
    source_value: Any
    target_value: Any
    message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        status = "✅ PASS" if self.passed else "❌ FAIL"
        return f"{status} [{self.check_type}] {self.message}"


@dataclass
class ValidationReport:
    """Complete validation report for a migration."""
    table_name: str
    timestamp: datetime
    checks: List[ValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failed_checks(self) -> List[ValidationResult]:
        return [c for c in self.checks if not c.passed]

    def summary(self) -> str:
        lines = [
            f"\n{'='*60}",
            f"Validation Report: {self.table_name}",
            f"{'='*60}",
        ]
        for check in self.checks:
            lines.append(f"  {check}")
        
        status = "✅ ALL CHECKS PASSED" if self.passed else f"❌ {len(self.failed_checks)} CHECK(S) FAILED"
        lines.append(f"\n{status}")
        lines.append(f"{'='*60}\n")
        return "\n".join(lines)


class DataValidator:
    """
    Validates data integrity between BigQuery source and PostgreSQL target.
    """

    def __init__(
        self,
        bq_client: bigquery.Client,
        pg_engine,
        bq_table_ref: str,
        pg_table_name: str,
    ):
        self.bq_client = bq_client
        self.pg_engine = pg_engine
        self.bq_table_ref = bq_table_ref
        self.pg_table_name = pg_table_name

    def validate_all(self) -> ValidationReport:
        """
        Run all configured validation checks.

        Returns
        -------
        ValidationReport
            Report containing row count validation results.
        """
        report = ValidationReport(
            table_name=self.pg_table_name,
            timestamp=datetime.now(),
        )

        result = self._validate_row_count()
        report.checks.append(result)

        return report

    def _validate_row_count(self) -> ValidationResult:
        """Compare row counts between BQ and PG."""
        try:
            # BigQuery count
            bq_query = f"SELECT COUNT(*) as cnt FROM `{self.bq_table_ref}`"
            bq_result = list(self.bq_client.query(bq_query).result())
            bq_count = bq_result[0].cnt if bq_result else 0

            # PostgreSQL count
            with self.pg_engine.connect() as conn:
                pg_result = conn.execute(
                    text(f'SELECT COUNT(*) as cnt FROM "{self.pg_table_name}"')
                )
                pg_count = pg_result.scalar() or 0

            passed = bq_count == pg_count
            diff = abs(bq_count - pg_count)
            diff_pct = (diff / bq_count * 100) if bq_count > 0 else 0

            return ValidationResult(
                passed=passed,
                check_type="row_count",
                source_value=bq_count,
                target_value=pg_count,
                message=f"BQ={bq_count:,} vs PG={pg_count:,}" + (
                    "" if passed else f" (diff={diff:,}, {diff_pct:.2f}%)"
                ),
                details={
                    "bq_count": bq_count,
                    "pg_count": pg_count,
                    "difference": diff,
                    "difference_pct": diff_pct,
                },
            )
        except Exception as e:
            return ValidationResult(
                passed=False,
                check_type="row_count",
                source_value=None,
                target_value=None,
                message=f"Row count validation failed: {str(e)[:100]}",
                details={"error": str(e)},
            )


