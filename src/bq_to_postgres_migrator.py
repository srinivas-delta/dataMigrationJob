import traceback
import json
from typing import Dict, List, Tuple, Any, Optional, Callable
from datetime import datetime
import time
from functools import wraps

from google.cloud import bigquery
from sqlalchemy import create_engine, text, insert, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
from google.oauth2 import service_account

from .type_mappings import (
    get_postgresql_type,
    get_type_category,
    needs_json_conversion,
    is_binary_type,
    get_all_supported_types,
)
from .schema_drift_handler import SchemaDriftHandler, SchemaDriftReport
from .validation import DataValidator, ValidationReport
from .progress_tracker import ProgressTracker


def retry_with_exponential_backoff(max_retries: int = 3, initial_delay: float = 1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        print(f"   ⚠️  Attempt {attempt + 1}/{max_retries + 1} failed: {str(e)[:100]}")
                        print(f"   ⏳ Retrying in {delay:.1f} seconds...")
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff
                    else:
                        print(f"   ❌ All {max_retries + 1} attempts failed")
            
            raise last_exception
        return wrapper
    return decorator


class BQTypeMapper:

    @classmethod
    def convert_type(cls, bq_type: str, mode: str = 'NULLABLE') -> str:
        return get_postgresql_type(bq_type, mode)
    
    @classmethod
    def get_type_category(cls, bq_type: str) -> str:
        return get_type_category(bq_type)
    
    @classmethod
    def needs_special_conversion(cls, bq_type: str, mode: str = 'NULLABLE') -> bool:
        return needs_json_conversion(bq_type, mode)
    
    @classmethod
    def get_all_supported_types(cls) -> dict:
        return get_all_supported_types()


class BQToPostgresMigrator:
    """Handles migration of tables from BigQuery to PostgreSQL"""
    
    def __init__(
        self,
        bq_table_ref: str,
        gcp_credentials: Any,
        pg_connection_string: str,
        primary_key: str = "id",
        removed_policy: str = "keep",
        validate: bool = True,
        show_progress: bool = True,
        progress_callback: Optional[Callable] = None,
    ):
        self.bq_table_ref = bq_table_ref
        self.project, self.dataset, self.table = self._parse_table_ref(bq_table_ref)
        self.gcp_credentials = gcp_credentials
        self.primary_key = primary_key
        self.removed_policy = removed_policy  # keep | nullify | drop
        
        # Validation and progress options
        self.validate = validate
        self.show_progress = show_progress
        self.progress_callback = progress_callback

        # Create engine with optimized connection pooling
        # pool_size: number of connections to keep in pool
        # max_overflow: number of additional connections beyond pool_size
        # pool_pre_ping: verify connections before using them
        self.pg_engine = create_engine(
            pg_connection_string,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600  # Recycle connections after 1 hour
        )

        self.bq_client = None
        self.pg_table_name = self.table.lower()

        # Pre-compute column placeholders for faster batch inserts
        self._column_placeholder_cache = {}
        
    def _parse_table_ref(self, table_ref: str) -> Tuple[str, str, str]:
        """Parse BigQuery table reference"""
        parts = table_ref.split('.')
        if len(parts) != 3:
            raise ValueError(f"Invalid table reference. Expected format: 'project.dataset.table', got: '{table_ref}'")
        return parts[0], parts[1], parts[2]

    def run_validation(self) -> ValidationReport:
        """
        Run post-migration validation (row count only).
        
        Returns ValidationReport with row count validation.
        """
        validator = DataValidator(
            bq_client=self._get_bq_client(),
            pg_engine=self.pg_engine,
            bq_table_ref=self.bq_table_ref,
            pg_table_name=self.pg_table_name,
        )
        return validator.validate_all()
    
    def _get_bq_client(self) -> bigquery.Client:
        """Get BigQuery client with credentials"""
        if not self.bq_client:
            self.bq_client = bigquery.Client(
                credentials=self.gcp_credentials,
                project=self.gcp_credentials.project_id
            )
        return self.bq_client
    
    def get_bq_table_schema(self) -> List[bigquery.SchemaField]:
        """Get BigQuery table schema"""
        try:
            client = self._get_bq_client()
            table = client.get_table(self.bq_table_ref)
            return table.schema
        except Exception as e:
            print(f"❌ Error fetching BigQuery table schema: {str(e)}")
            raise Exception(f"Failed to get schema for {self.bq_table_ref}: {str(e)}")
    
    def generate_postgres_ddl(self, schema: List[bigquery.SchemaField]) -> str:
        """Generate PostgreSQL DDL from BigQuery schema.
        
        Validates schema is not empty to prevent creating empty tables.
        """
        if not schema:
            raise ValueError(
                f"Cannot create DDL: BigQuery table '{self.bq_table_ref}' has no columns. "
                f"Verify the table exists and has columns defined."
            )
        
        columns = []
        
        for field in schema:
            field_name = field.name.lower()
            # Use new mapper that handles NOT NULL and REPEATED
            pg_type = BQTypeMapper.convert_type(field.field_type, field.mode)
            
            columns.append(f'    "{field_name}" {pg_type}')
        
        ddl = f'CREATE TABLE "{self.pg_table_name}" (\n'
        ddl += ',\n'.join(columns)
        ddl += '\n);'
        
        return ddl
    
    def drop_postgres_table(self):
        """Drop PostgreSQL table if it exists, with retry logic"""
        try:
            max_retries = 3
            for attempt in range(max_retries + 1):
                try:
                    with self.pg_engine.connect() as conn:
                        conn.execute(text(f'DROP TABLE IF EXISTS "{self.pg_table_name}" CASCADE;'))
                        conn.commit()
                    print(f"✅ Dropped table '{self.pg_table_name}' (if existed)")
                    return
                except Exception as e:
                    error_msg = str(e)[:100]
                    is_transient = any(
                        keyword in error_msg.lower() 
                        for keyword in ['connection', 'timeout', 'temporarily', 'unavailable', 'refused', 'recovery']
                    )
                    
                    if is_transient and attempt < max_retries:
                        delay = 2 ** attempt
                        print(f"   ⚠️  Drop table attempt {attempt + 1}/{max_retries + 1} failed: {error_msg}")
                        print(f"      ⏳ Retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        raise
        except Exception as e:
            print(f"❌ Error dropping PostgreSQL table: {str(e)}")
            raise Exception(f"Failed to drop table {self.pg_table_name}: {str(e)}")
    
    def create_postgres_table(self, ddl: str):
        """Create PostgreSQL table using DDL, with retry logic.
        
        Args:
            ddl: The CREATE TABLE statement
            
        Raises:
            Exception: If table creation fails after all retries
        """
        try:
            # Validate DDL format
            if not ddl.strip().upper().startswith('CREATE TABLE'):
                raise ValueError(f"Invalid DDL format. Expected CREATE TABLE statement, got:\n{ddl}")
            
            # Check for column count
            column_count = ddl.count(',\n') + (1 if '    "' in ddl else 0)
            if column_count == 0:
                raise ValueError(
                    f"DDL appears to have no columns. Table structure:\n{ddl}\n"
                    f"Verify BigQuery schema contains columns."
                )
            
            if self.show_progress:
                print(f"📝 DDL ({column_count} columns):\n{ddl}\n")
            
            max_retries = 3
            for attempt in range(max_retries + 1):
                try:
                    with self.pg_engine.connect() as conn:
                        conn.execute(text(ddl))
                        conn.commit()
                    print(f"✅ Created table '{self.pg_table_name}' with {column_count} columns")
                    return
                except Exception as e:
                    error_msg = str(e)[:100]
                    is_transient = any(
                        keyword in error_msg.lower() 
                        for keyword in ['connection', 'timeout', 'temporarily', 'unavailable', 'refused', 'recovery']
                    )
                    
                    if is_transient and attempt < max_retries:
                        delay = 2 ** attempt
                        print(f"   ⚠️  Create table attempt {attempt + 1}/{max_retries + 1} failed: {error_msg}")
                        print(f"      ⏳ Retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        raise
        except Exception as e:
            print(f"❌ Error creating PostgreSQL table: {str(e)}")
            raise Exception(f"Failed to create table {self.pg_table_name}: {str(e)}")
    
    def _convert_value(self, value: Any, field_type: str) -> Any:
        if value is None:
            return None
        
        # Check if field needs JSON conversion using centralized config
        if needs_json_conversion(field_type):
            return json.dumps(value) if value else None
        
        # Handle BYTES type using optimized lookup
        if is_binary_type(field_type):
            return bytes(value) if value else None
        
        return value
    
    def _convert_field_value(self, value: Any, field_type: str, 
                            needs_conversion: bool, is_binary: bool) -> Any:
        if value is None:
            return None
        
        if needs_conversion:
            return json.dumps(value) if value else None
        
        if is_binary:
            return bytes(value) if value else None
        
        return value
    
    def transfer_data(self, batch_size: Optional[int] = None):

        try:
            total_start = time.perf_counter()
            
            client = self._get_bq_client()
            
            # Get table schema to understand field types
            table = client.get_table(self.bq_table_ref)
            schema = table.schema
            
            # OPTIMIZATION: Pre-compute field info to avoid repeated lookups
            field_info = [
                {
                    'name': field.name,
                    'name_lower': field.name.lower(),
                    'type': field.field_type,
                    'needs_conversion': needs_json_conversion(field.field_type),
                    'is_binary': is_binary_type(field.field_type),
                }
                for field in schema
            ]
            
            # Query all data from BigQuery
            query = f"SELECT * FROM `{self.bq_table_ref}`"
            print(f"📊 Fetching data from BigQuery: {self.bq_table_ref}")
            
            bq_fetch_start = time.perf_counter()
            query_job = client.query(query)
            results = query_job.result()  # This is where streaming results are fetched
            bq_results_list = list(results)  # Fully load results
            bq_fetch_time = time.perf_counter() - bq_fetch_start
            print(f"   ⏱️  BigQuery fetch time: {bq_fetch_time:.2f}s for {len(bq_results_list)} rows")
            
            # Get column names
            columns = [f['name_lower'] for f in field_info]
            column_str = ', '.join([f'"{col}"' for col in columns])
            
            # Prepare for batch insert
            rows_transferred = 0
            batch = []
            failed_batches = []
            
            conversion_start = time.perf_counter()
            conversion_time_total = 0
            insert_time_total = 0
            
            with self.pg_engine.connect() as conn:
                batch_num = 0
                for row in bq_results_list:
                    # Convert row to dict and handle type conversions
                    row_dict = dict(row)
                    
                    # OPTIMIZATION: Use pre-computed field info
                    converted_row = tuple(
                        self._convert_field_value(
                            row_dict.get(info['name']),
                            info['type'],
                            info['needs_conversion'],
                            info['is_binary']
                        )
                        for info in field_info
                    )
                    batch.append(converted_row)
                    
                    # Insert batch when it reaches batch_size (if batching enabled)
                    if batch_size is not None and len(batch) >= batch_size:
                        batch_num += 1
                        try:
                            insert_start = time.perf_counter()
                            self._insert_batch(conn, column_str, columns, batch, max_retries=3)
                            insert_time_total += time.perf_counter() - insert_start
                            
                            rows_transferred += len(batch)
                            print(f"  ✅ Batch {batch_num}: Transferred {rows_transferred} rows...")
                        except Exception as batch_error:
                            failed_batches.append({
                                'batch_num': batch_num,
                                'batch_size': len(batch),
                                'error': str(batch_error)[:200]
                            })
                            print(f"  ❌ Batch {batch_num} FAILED after retries: {str(batch_error)[:100]}")
                        
                        batch = []
                
                # Insert remaining rows
                if batch:
                    batch_num += 1
                    try:
                        insert_start = time.perf_counter()
                        self._insert_batch(conn, column_str, columns, batch, max_retries=3)
                        insert_time_total += time.perf_counter() - insert_start
                        
                        rows_transferred += len(batch)
                        print(f"  ✅ Batch {batch_num} (final): Transferred {rows_transferred} rows...")
                    except Exception as batch_error:
                        failed_batches.append({
                            'batch_num': batch_num,
                            'batch_size': len(batch),
                            'error': str(batch_error)[:200]
                        })
                        print(f"  ❌ Final batch {batch_num} FAILED after retries: {str(batch_error)[:100]}")
                
                conversion_time_total = time.perf_counter() - conversion_start - insert_time_total
                conn.connection.commit()
            
            total_time = time.perf_counter() - total_start
            
            print(f"✅ Transferred {rows_transferred} rows to '{self.pg_table_name}'")
            print(f"\n⏱️  Performance Breakdown:")
            print(f"   BigQuery fetch:   {bq_fetch_time:.2f}s ({bq_fetch_time/total_time*100:.1f}%)")
            print(f"   Row conversion:   {conversion_time_total:.2f}s ({conversion_time_total/total_time*100:.1f}%)")
            print(f"   DB insertion:     {insert_time_total:.2f}s ({insert_time_total/total_time*100:.1f}%)")
            print(f"   ---")
            print(f"   TOTAL:            {total_time:.2f}s")
            
            # Report failed batches if any
            if failed_batches:
                print(f"\n⚠️  FAILED BATCHES: {len(failed_batches)}")
                for fb in failed_batches:
                    print(f"   - Batch {fb['batch_num']}: {fb['batch_size']} rows - {fb['error']}")
                raise Exception(f"{len(failed_batches)} batches failed after all retry attempts")
            
            return rows_transferred
            
        except Exception as e:
            print(f"❌ Error transferring data: {str(e)}")
            raise Exception(f"Failed to transfer data: {str(e)}")
    
    def _insert_batch(self, conn, column_str: str, columns: List[str], batch: List[tuple], 
                     retry_count: int = 0, max_retries: int = 3):

        if not batch:
            return
        
        # Build multi-row INSERT with placeholders
        num_columns = len(columns)
        num_rows = len(batch)

        # Create placeholders: (%s, %s, ...), (%s, %s, ...), ...
        row_placeholders = ', '.join(['%s'] * num_columns)
        all_placeholders = ', '.join([f'({row_placeholders})' for _ in range(num_rows)])

        # Escape % in column names so psycopg2 doesn't treat them as param placeholders
        # (e.g. column 'discount_%_eqn' would otherwise consume an extra %s argument)
        safe_column_str = column_str.replace('%', '%%')

        # Single INSERT statement with all rows
        insert_sql = f'INSERT INTO "{self.pg_table_name}" ({safe_column_str}) VALUES {all_placeholders}'
        
        # Flatten batch into single list of values for parameters
        flat_values = []
        for row in batch:
            flat_values.extend(row)
        
        # Execute using raw psycopg2 cursor for maximum performance
        try:
            raw_conn = conn.connection.connection
            cursor = raw_conn.cursor()
            cursor.execute(insert_sql, flat_values)
            cursor.close()
        except Exception as e:
            error_msg = str(e)[:150]
            
            # Check if this is a connection/timeout error (worth retrying)
            is_transient = any(
                keyword in error_msg.lower() 
                for keyword in ['connection', 'timeout', 'temporarily', 'unavailable', 'recovery', 'deadlock']
            )
            
            if is_transient and retry_count < max_retries:
                # Transient error - retry entire batch with backoff
                delay = 2 ** retry_count  # Exponential: 1s, 2s, 4s, 8s
                print(f"   ⚠️  Batch insert failed (transient error), attempt {retry_count + 1}/{max_retries}")
                print(f"      Error: {error_msg}")
                print(f"      ⏳ Retrying batch of {len(batch)} rows in {delay}s...")
                time.sleep(delay)
                
                # Retry the entire batch
                return self._insert_batch(conn, column_str, columns, batch, retry_count + 1, max_retries)
            
            elif len(batch) > 1 and not is_transient:
                # Data error with multiple rows - try row-by-row recovery
                print(f"   ⚠️  Batch insert failed (data error), attempting row-by-row recovery")
                print(f"      Error: {error_msg}")
                print(f"      Inserting {len(batch)} rows individually...")
                
                successful = 0
                failed_rows = []
                
                for idx, row in enumerate(batch):
                    try:
                        single_batch = [row]
                        num_cols = len(columns)
                        row_ph = ', '.join(['%s'] * num_cols)
                        safe_col_str = column_str.replace('%', '%%')
                        single_sql = f'INSERT INTO "{self.pg_table_name}" ({safe_col_str}) VALUES ({row_ph})'
                        
                        raw_conn = conn.connection.connection
                        cursor = raw_conn.cursor()
                        cursor.execute(single_sql, row)
                        cursor.close()
                        successful += 1
                    except Exception as row_error:
                        failed_rows.append((idx, row, str(row_error)[:100]))
                        print(f"      ❌ Row {idx + 1}/{len(batch)} failed: {str(row_error)[:80]}")
                
                if failed_rows:
                    print(f"   ⚠️  {successful}/{len(batch)} rows inserted, {len(failed_rows)} failed")
                    error_details = '\n'.join([
                        f"      Row {idx + 1}: {err}" 
                        for idx, _, err in failed_rows
                    ])
                    raise Exception(f"Row-level failures during recovery:\n{error_details}")
                else:
                    print(f"   ✅ Successfully recovered all {successful} rows via row-by-row insert")
                    return
            else:
                # Single row or permanent error - give up
                raise

    
    # ------------------------------------------------------------------
    # Safe incremental migrate (drift-aware, no truncate, upsert)
    # ------------------------------------------------------------------

    def migrate_safe(
        self,
        batch_size: Optional[int] = None,
        watermark_col: Optional[str] = None,
        watermark_value: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Drift-aware, live-safe migration:
          1. Detect schema changes (add/remove/type) between BQ and PG
          2. Apply non-destructive DDL (ALTER TABLE ADD/DROP COLUMN)
          3. Upsert only changed rows using ON CONFLICT (primary_key) DO UPDATE

        Parameters
        ----------
        batch_size      : Rows per upsert batch
        watermark_col   : BQ timestamp column to filter delta rows (e.g. 'updated_at')
        watermark_value : ISO timestamp string; only rows newer than this are fetched
        """
        print(f"\n{'='*60}")
        print(f"[SAFE] Migration: {self.bq_table_ref} → {self.pg_table_name}")
        if watermark_col and watermark_value:
            print(f"       Incremental: {watermark_col} > {watermark_value}")
        print(f"{'='*60}\n")

        start_time = datetime.now()

        try:
            # Step 1: BQ schema
            print("📋 Step 1: Fetching BigQuery schema...")
            schema = self.get_bq_table_schema()
            print(f"   Found {len(schema)} columns in BQ")

            # Step 2: Schema drift detection + DDL apply
            print("\n🔍 Step 2: Detecting schema drift...")
            drift_handler = SchemaDriftHandler(
                pg_engine=self.pg_engine,
                pg_table_name=self.pg_table_name,
                removed_policy=self.removed_policy,
            )
            drift_report = drift_handler.detect_and_apply(schema)
            print(drift_report.summary())

            # Step 3: Create table if it didn't exist
            if not drift_report.table_existed:
                print("\n🏗️  Step 3: Table missing — creating from BQ schema...")
                ddl = self.generate_postgres_ddl(schema)
                self.create_postgres_table(ddl)
            else:
                print(f"\n✅ Step 3: Table exists — skipping CREATE ({len(drift_report.added)} columns added, {len(drift_report.dropped)} policy-handled)")

            # Step 4: Upsert delta rows
            print("\n📦 Step 4: Upserting data (no truncate)...")
            active_columns = drift_report.active_bq_columns
            rows_upserted = self.transfer_data_upsert(
                batch_size=batch_size,
                active_columns=active_columns,
                watermark_col=watermark_col,
                watermark_value=watermark_value,
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "success": True,
                "mode": "safe_upsert",
                "bq_table": self.bq_table_ref,
                "pg_table": self.pg_table_name,
                "rows_upserted": rows_upserted,
                "columns_active": len(active_columns),
                "columns_added": len(drift_report.added),
                "columns_dropped_policy": len(drift_report.dropped),
                "type_mismatches": len(drift_report.type_mismatches),
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "drift_summary": drift_report.summary(),
            }

            print(f"\n{'='*60}")
            print(f"✅ Safe migration complete! {rows_upserted} rows upserted in {duration:.2f}s")
            print(f"{'='*60}\n")
            return result

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            result = {
                "success": False,
                "mode": "safe_upsert",
                "bq_table": self.bq_table_ref,
                "pg_table": self.pg_table_name,
                "error": str(e),
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            }
            print(f"\n❌ Safe migration failed: {str(e)}")
            raise

    def transfer_data_upsert(
        self,
        batch_size: Optional[int] = None,
        active_columns: Optional[List[str]] = None,
        watermark_col: Optional[str] = None,
        watermark_value: Optional[str] = None,
    ) -> int:
        """
        Fetch rows from BQ (optionally filtered by watermark) and upsert
        into PG using INSERT ... ON CONFLICT (primary_key) DO UPDATE.

        Only columns present in `active_columns` are synced — so schema
        drift (extra/missing columns) is handled gracefully.
        """
        try:
            total_start = time.perf_counter()
            client = self._get_bq_client()

            # Full BQ schema for type conversion info
            bq_table = client.get_table(self.bq_table_ref)
            full_schema = bq_table.schema

            # Filter schema to only active columns
            if active_columns:
                active_set = set(active_columns)
                schema = [f for f in full_schema if f.name.lower() in active_set]
            else:
                schema = full_schema
                active_columns = [f.name.lower() for f in schema]

            # Pre-compute field conversion info
            field_info = [
                {
                    "name": f.name,
                    "name_lower": f.name.lower(),
                    "type": f.field_type,
                    "needs_conversion": needs_json_conversion(f.field_type),
                    "is_binary": is_binary_type(f.field_type),
                }
                for f in schema
            ]

            # Build BQ query — incremental if watermark provided
            if watermark_col and watermark_value:
                cols_str = ", ".join(f"`{f.name}`" for f in schema)
                query = (
                    f"SELECT {cols_str} "
                    f"FROM `{self.bq_table_ref}` "
                    f"WHERE `{watermark_col}` > TIMESTAMP('{watermark_value}')"
                )
                print(f"📊 Incremental BQ fetch (delta only): {watermark_col} > {watermark_value}")
            else:
                cols_str = ", ".join(f"`{f.name}`" for f in schema)
                query = f"SELECT {cols_str} FROM `{self.bq_table_ref}`"
                print(f"📊 Full BQ fetch ({len(schema)} columns): {self.bq_table_ref}")

            # Pre-compute columns — needed for effective_batch_size calculation
            columns = [fi["name_lower"] for fi in field_info]
            column_str = ", ".join(f'"{c}"' for c in columns)

            # Build ON CONFLICT upsert SET clause — skip primary key
            set_clause = ", ".join(
                f'"{c}" = EXCLUDED."{c}"'
                for c in columns
                if c != self.primary_key
            )

            # psycopg2 hard-caps at 65535 parameters per statement.
            # Derive a safe row-per-batch limit so we never exceed it.
            MAX_PG_PARAMS = 50_000  # conservative margin below the 65535 ceiling
            effective_batch_size = batch_size if batch_size is not None else max(50, MAX_PG_PARAMS // max(1, len(columns)))
            print(f"   📦 Batch size: {effective_batch_size} rows  ({len(columns)} cols × {effective_batch_size} = {len(columns) * effective_batch_size:,} params)")

            bq_start = time.perf_counter()
            bq_result_iter = client.query(query).result()
            if bq_result_iter.total_rows is not None and bq_result_iter.total_rows == 0:
                print("   ℹ️  No rows to upsert (empty delta).")
                return 0
            if bq_result_iter.total_rows:
                print(f"   📊 Total rows: {bq_result_iter.total_rows:,}")

            rows_upserted = 0
            insert_time_total = 0.0

            with self.pg_engine.connect() as conn:
                batch: List[tuple] = []
                batch_num = 0

                for row in bq_result_iter:
                    row_dict = dict(row)
                    converted = tuple(
                        self._convert_field_value(
                            row_dict.get(fi["name"]),
                            fi["type"],
                            fi["needs_conversion"],
                            fi["is_binary"],
                        )
                        for fi in field_info
                    )
                    batch.append(converted)

                    if len(batch) >= effective_batch_size:
                        batch_num += 1
                        t0 = time.perf_counter()
                        self._upsert_batch(
                            conn, column_str, columns, set_clause, batch
                        )
                        insert_time_total += time.perf_counter() - t0
                        rows_upserted += len(batch)
                        print(f"  ✅ Batch {batch_num}: {rows_upserted:,} rows upserted...")
                        batch = []

                if batch:
                    batch_num += 1
                    t0 = time.perf_counter()
                    self._upsert_batch(conn, column_str, columns, set_clause, batch)
                    insert_time_total += time.perf_counter() - t0
                    rows_upserted += len(batch)
                    print(f"  ✅ Batch {batch_num} (final): {rows_upserted:,} rows upserted")

                conn.connection.commit()

            total_elapsed = time.perf_counter() - bq_start
            total_time = time.perf_counter() - total_start
            print(f"\n⏱️  Upsert breakdown:")
            print(f"   BQ+PG streamed: {total_elapsed:.2f}s  |  PG upsert: {insert_time_total:.2f}s  |  Total: {total_time:.2f}s")
            return rows_upserted

        except Exception as e:
            print(f"❌ Upsert failed: {str(e)}")
            raise

    def _upsert_batch(
        self,
        conn,
        column_str: str,
        columns: List[str],
        set_clause: str,
        batch: List[tuple],
        retry_count: int = 0,
        max_retries: int = 3,
    ):
        """INSERT ... ON CONFLICT (pk) DO UPDATE SET ... for a batch of rows."""
        if not batch:
            return

        num_cols = len(columns)
        row_ph = ", ".join(["%s"] * num_cols)
        all_ph = ", ".join([f"({row_ph})" for _ in batch])

        # Escape % in column names and SET clause so psycopg2 doesn't misparse them
        safe_column_str = column_str.replace('%', '%%')
        safe_set_clause = set_clause.replace('%', '%%')

        upsert_sql = (
            f'INSERT INTO "{self.pg_table_name}" ({safe_column_str}) '
            f"VALUES {all_ph} "
            f'ON CONFLICT ("{self.primary_key}") DO UPDATE SET {safe_set_clause}'
        )

        flat_values = [v for row in batch for v in row]

        try:
            raw_conn = conn.connection.connection
            cursor = raw_conn.cursor()
            cursor.execute(upsert_sql, flat_values)
            cursor.close()
        except Exception as e:
            error_msg = str(e)[:150]
            is_transient = any(
                kw in error_msg.lower()
                for kw in ["connection", "timeout", "temporarily", "unavailable", "recovery", "deadlock"]
            )
            if is_transient and retry_count < max_retries:
                delay = 2 ** retry_count
                print(f"   ⚠️  Upsert batch transient error (attempt {retry_count + 1}/{max_retries}), retrying in {delay}s...")
                time.sleep(delay)
                return self._upsert_batch(conn, column_str, columns, set_clause, batch, retry_count + 1, max_retries)
            raise

    # ------------------------------------------------------------------
    # Watermark-based append (no PK required — plain INSERT)
    # ------------------------------------------------------------------

    def migrate_append(
        self,
        batch_size: Optional[int] = None,
        watermark_col: Optional[str] = None,
        watermark_value: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Incremental append migration for tables with no primary key:
          1. CREATE TABLE IF NOT EXISTS from BQ schema (never drops existing rows)
          2. Fetch only rows WHERE watermark_col > last_watermark
          3. Plain INSERT — no ON CONFLICT needed

        Safe to run repeatedly — only new rows since the last watermark are added.
        """
        print(f"\n{'='*60}")
        print(f"[APPEND] Migration: {self.bq_table_ref} → {self.pg_table_name}")
        if watermark_col and watermark_value:
            print(f"         Incremental: {watermark_col} > {watermark_value}")
        else:
            print(f"         No watermark — loading ALL rows (first run)")
        print(f"{'='*60}\n")

        start_time = datetime.now()

        try:
            # Step 1: BQ schema
            print("\U0001f4cb Step 1: Fetching BigQuery schema...")
            schema = self.get_bq_table_schema()
            print(f"   Found {len(schema)} columns in BQ")

            # Step 2: Create table only if it doesn't exist (no DROP)
            print("\n\U0001f3d7\ufe0f  Step 2: Ensuring PostgreSQL table exists...")
            ddl = self.generate_postgres_ddl(schema)
            ddl_safe = ddl.replace('CREATE TABLE "', 'CREATE TABLE IF NOT EXISTS "', 1)
            with self.pg_engine.connect() as conn:
                conn.execute(text(ddl_safe))
                conn.commit()
            print(f"   Table '{self.pg_table_name}' ready (existing rows preserved)")

            # Step 3: Append new rows
            print("\n\U0001f4e6 Step 3: Appending delta rows...")
            rows_inserted = self.transfer_data_append(
                batch_size=batch_size,
                watermark_col=watermark_col,
                watermark_value=watermark_value,
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print(f"\n{'='*60}")
            print(f"\u2705 Append complete — {rows_inserted:,} new rows added")
            print(f"   Duration: {duration:.2f}s")
            print(f"{'='*60}\n")

            return {
                'success': True,
                'mode': 'watermark_append',
                'bq_table': self.bq_table_ref,
                'pg_table': self.pg_table_name,
                'rows_upserted': rows_inserted,
                'rows_transferred': rows_inserted,
                'columns': len(schema),
                'duration_seconds': duration,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
            }

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            print(f"\n\u274c Append failed: {str(e)}")
            return {
                'success': False,
                'mode': 'watermark_append',
                'bq_table': self.bq_table_ref,
                'pg_table': self.pg_table_name,
                'error': str(e),
                'duration_seconds': duration,
            }

    def transfer_data_append(
        self,
        batch_size: Optional[int] = None,
        watermark_col: Optional[str] = None,
        watermark_value: Optional[str] = None,
    ) -> int:
        """Plain INSERT of delta rows. No ON CONFLICT — for tables without a primary key."""
        total_start = time.perf_counter()
        try:
            client = self._get_bq_client()
            schema = self.get_bq_table_schema()

            field_info = [
                {
                    "name": f.name,
                    "name_lower": f.name.lower(),
                    "type": f.field_type,
                    "needs_conversion": needs_json_conversion(f.field_type),
                    "is_binary": is_binary_type(f.field_type),
                }
                for f in schema
            ]

            cols_str = ", ".join(f"`{f.name}`" for f in schema)
            if watermark_col and watermark_value:
                query = (
                    f"SELECT {cols_str} FROM `{self.bq_table_ref}` "
                    f"WHERE `{watermark_col}` > TIMESTAMP('{watermark_value}')"
                )
                print(f"\U0001f4ca Incremental BQ fetch: {watermark_col} > {watermark_value}")
            else:
                query = f"SELECT {cols_str} FROM `{self.bq_table_ref}`"
                print(f"\U0001f4ca Full BQ fetch (first run): {self.bq_table_ref}")

            # Pre-compute columns — needed for effective_batch_size calculation
            columns = [fi["name_lower"] for fi in field_info]
            column_str = ", ".join(f'"{c}"' for c in columns)

            # psycopg2 hard-caps at 65535 parameters per statement.
            # Derive a safe row-per-batch limit so we never exceed it.
            MAX_PG_PARAMS = 50_000  # conservative margin below the 65535 ceiling
            effective_batch_size = batch_size if batch_size is not None else max(50, MAX_PG_PARAMS // max(1, len(columns)))
            print(f"   \U0001f4e6 Batch size: {effective_batch_size} rows  ({len(columns)} cols × {effective_batch_size} = {len(columns) * effective_batch_size:,} params)")

            bq_start = time.perf_counter()
            bq_result_iter = client.query(query).result()
            if bq_result_iter.total_rows is not None and bq_result_iter.total_rows == 0:
                print("   \u2139\ufe0f  No new rows since last watermark.")
                return 0
            if bq_result_iter.total_rows:
                print(f"   \U0001f4ca Total rows: {bq_result_iter.total_rows:,}")

            rows_inserted = 0
            insert_time_total = 0.0

            with self.pg_engine.connect() as conn:
                batch: List[tuple] = []
                batch_num = 0

                for row in bq_result_iter:
                    row_dict = dict(row)
                    converted = tuple(
                        self._convert_field_value(
                            row_dict.get(fi["name"]),
                            fi["type"],
                            fi["needs_conversion"],
                            fi["is_binary"],
                        )
                        for fi in field_info
                    )
                    batch.append(converted)

                    if len(batch) >= effective_batch_size:
                        batch_num += 1
                        t0 = time.perf_counter()
                        self._insert_batch(conn, column_str, columns, batch, max_retries=3)
                        insert_time_total += time.perf_counter() - t0
                        rows_inserted += len(batch)
                        print(f"  \u2705 Batch {batch_num}: {rows_inserted:,} rows inserted...")
                        batch = []

                if batch:
                    batch_num += 1
                    t0 = time.perf_counter()
                    self._insert_batch(conn, column_str, columns, batch, max_retries=3)
                    insert_time_total += time.perf_counter() - t0
                    rows_inserted += len(batch)
                    print(f"  \u2705 Batch {batch_num} (final): {rows_inserted:,} rows inserted")

                conn.connection.commit()

            total_elapsed = time.perf_counter() - bq_start
            total_time = time.perf_counter() - total_start
            print(f"\n\u23f1\ufe0f  Append breakdown:")
            print(f"   BQ+PG streamed: {total_elapsed:.2f}s  |  PG insert: {insert_time_total:.2f}s  |  Total: {total_time:.2f}s")
            return rows_inserted

        except Exception as e:
            print(f"\u274c Append transfer failed: {str(e)}")
            raise

    def migrate(self, batch_size: Optional[int] = None) -> Dict[str, Any]:
        print(f"\n{'='*60}")
        print(f"Starting migration: {self.bq_table_ref} → {self.pg_table_name}")
        print(f"{'='*60}\n")
        
        start_time = datetime.now()
        
        try:
            # Step 1: Get BigQuery schema
            print("📋 Step 1: Fetching BigQuery table schema...")
            schema = self.get_bq_table_schema()
            print(f"   Found {len(schema)} columns")
            
            # Step 2: Generate PostgreSQL DDL
            print("\n🔧 Step 2: Generating PostgreSQL DDL...")
            ddl = self.generate_postgres_ddl(schema)
            print("   DDL generated:")
            for line in ddl.split('\n')[:5]:  # Show first 5 lines
                print(f"   {line}")
            if len(ddl.split('\n')) > 5:
                print(f"   ... ({len(ddl.split('\n')) - 5} more lines)")
            
            # Step 3: Drop existing table
            print("\n🗑️  Step 3: Dropping existing PostgreSQL table...")
            self.drop_postgres_table()
            
            # Step 4: Create new table
            print("\n🏗️  Step 4: Creating PostgreSQL table...")
            self.create_postgres_table(ddl)
            
            # Step 5: Transfer data
            print("\n📦 Step 5: Transferring data...")
            rows_transferred = self.transfer_data(batch_size)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'success': True,
                'bq_table': self.bq_table_ref,
                'pg_table': self.pg_table_name,
                'rows_transferred': rows_transferred,
                'columns': len(schema),
                'duration_seconds': duration,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
            
            print(f"\n{'='*60}")
            print(f"✅ Migration completed successfully!")
            print(f"   Rows transferred: {rows_transferred}")
            print(f"   Duration: {duration:.2f} seconds")
            print(f"{'='*60}\n")
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'success': False,
                'bq_table': self.bq_table_ref,
                'pg_table': self.pg_table_name,
                'error': str(e),
                'duration_seconds': duration,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
            
            print(f"\n{'='*60}")
            print(f"❌ Migration failed!")
            print(f"   Error: {str(e)}")
            print(f"{'='*60}\n")
            
            raise

    # ------------------------------------------------------------------
    # Atomic swap migration (full reload with zero-downtime swap)
    # ------------------------------------------------------------------

    def migrate_atomic_swap(self, batch_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Full reload migration with atomic table swap for tables WITHOUT primary key
        and WITHOUT watermark column.

        Strategy:
          1. Create a temp table (e.g., "tablename_tmp") from BQ schema
          2. Load ALL data from BigQuery into the temp table
          3. Atomic swap in single transaction:
             - DROP main table (IF EXISTS CASCADE)
             - RENAME temp table to main table name
          
        Benefits:
          - Zero downtime: Old table serves queries while new data loads
          - Atomic switch: DROP + RENAME happens in ~milliseconds
          - Safe rollback: If load fails, main table is untouched
          - Clean slate: No duplicate/stale row issues
        """
        temp_table_name = f"{self.pg_table_name}_tmp"
        
        print(f"\n{'='*60}")
        print(f"[ATOMIC SWAP] Migration: {self.bq_table_ref} → {self.pg_table_name}")
        print(f"              Temp table: {temp_table_name}")
        print(f"{'='*60}\n")

        start_time = datetime.now()

        try:
            # Step 1: Get BigQuery schema
            print("📋 Step 1: Fetching BigQuery table schema...")
            schema = self.get_bq_table_schema()
            print(f"   Found {len(schema)} columns in BQ")

            # Step 2: Detect schema drift from existing table (for reporting & tracking)
            print("\n🔍 Step 2: Detecting schema drift from existing table...")
            drift_handler = SchemaDriftHandler(
                pg_engine=self.pg_engine,
                pg_table_name=self.pg_table_name,
                removed_policy=self.removed_policy,
            )
            drift_report = drift_handler.detect_and_apply(schema)
            print(drift_report.summary())
            print("   (Note: Atomic swap will replace entire table, but drift tracking is recorded)")

            # Step 3: Drop temp table if exists (from previous failed run)
            print(f"\n🧹 Step 3: Cleaning up any existing temp table...")
            with self.pg_engine.connect() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table_name}" CASCADE;'))
                conn.commit()
            print(f"   Temp table '{temp_table_name}' cleared")

            # Step 4: Create temp table with BQ schema
            print(f"\n🏗️  Step 4: Creating temp table '{temp_table_name}'...")
            ddl = self.generate_postgres_ddl(schema)
            # Replace main table name with temp table name in DDL
            temp_ddl = ddl.replace(f'"{self.pg_table_name}"', f'"{temp_table_name}"', 1)
            with self.pg_engine.connect() as conn:
                conn.execute(text(temp_ddl))
                conn.commit()
            print(f"   Temp table created")

            # Step 5: Load ALL data from BQ into temp table
            print(f"\n📦 Step 5: Loading data into temp table...")
            rows_transferred = self._transfer_data_to_table(
                target_table=temp_table_name,
                batch_size=batch_size,
            )
            print(f"   Loaded {rows_transferred:,} rows into temp table")

            # Step 6: ATOMIC SWAP — DROP main + RENAME temp in single transaction
            print(f"\n⚡ Step 6: Atomic swap (DROP + RENAME in single transaction)...")
            swap_start = time.perf_counter()
            
            with self.pg_engine.connect() as conn:
                # Use raw connection for explicit transaction control
                raw_conn = conn.connection.connection
                cursor = raw_conn.cursor()
                
                try:
                    # Start explicit transaction (SERIALIZABLE for maximum isolation)
                    cursor.execute("BEGIN")
                    
                    # Lock the temp table to prevent any concurrent access during swap
                    cursor.execute(f'LOCK TABLE "{temp_table_name}" IN ACCESS EXCLUSIVE MODE')
                    
                    # Drop the main table (if exists)
                    cursor.execute(f'DROP TABLE IF EXISTS "{self.pg_table_name}" CASCADE')
                    
                    # Rename temp table to main table
                    cursor.execute(f'ALTER TABLE "{temp_table_name}" RENAME TO "{self.pg_table_name}"')
                    
                    # Commit the atomic swap
                    cursor.execute("COMMIT")
                    
                except Exception as swap_error:
                    cursor.execute("ROLLBACK")
                    raise Exception(f"Atomic swap failed: {str(swap_error)}")
                finally:
                    cursor.close()
            
            swap_time_ms = (time.perf_counter() - swap_start) * 1000
            print(f"   ✅ Swap completed in {swap_time_ms:.2f}ms (DROP + RENAME)")

            # Step 7: Validation (if enabled)
            validation_result = None
            if self.validate:
                print(f"\n🔍 Step 7: Running post-migration validation...")
                validation_report = self.run_validation()
                print(validation_report.summary())
                validation_result = {
                    'passed': validation_report.passed,
                    'checks': [
                        {
                            'type': c.check_type,
                            'passed': c.passed,
                            'message': c.message,
                            'details': c.details,
                        }
                        for c in validation_report.checks
                    ],
                }
                if not validation_report.passed:
                    print("⚠️  Validation failed but migration completed. Review the data.")

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                'success': True,
                'mode': 'atomic_swap',
                'bq_table': self.bq_table_ref,
                'pg_table': self.pg_table_name,
                'rows_transferred': rows_transferred,
                'rows_upserted': rows_transferred,  # For compatibility with watermark logging
                'columns': len(schema),
                'columns_added': len(drift_report.added),
                'columns_dropped_policy': len(drift_report.dropped),
                'type_mismatches': len(drift_report.type_mismatches),
                'drift_summary': drift_report.summary(),
                'swap_time_ms': swap_time_ms,
                'duration_seconds': duration,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'validation': validation_result,
            }

            print(f"\n{'='*60}")
            print(f"✅ Atomic swap migration complete!")
            print(f"   Rows: {rows_transferred:,}  |  Swap: {swap_time_ms:.2f}ms  |  Total: {duration:.2f}s")
            if validation_result:
                status = "✅ PASSED" if validation_result['passed'] else "⚠️ FAILED"
                print(f"   Validation: {status}")
            print(f"{'='*60}\n")
            return result

        except Exception as e:
            # Cleanup: drop temp table if it exists (don't leave orphans)
            try:
                with self.pg_engine.connect() as conn:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table_name}" CASCADE;'))
                    conn.commit()
                print(f"   🧹 Cleaned up temp table after failure")
            except:
                pass  # Ignore cleanup errors

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"\n❌ Atomic swap migration failed: {str(e)}")
            return {
                'success': False,
                'mode': 'atomic_swap',
                'bq_table': self.bq_table_ref,
                'pg_table': self.pg_table_name,
                'error': str(e),
                'duration_seconds': duration,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
            }

    def _transfer_data_to_table(
        self,
        target_table: str,
        batch_size: Optional[int] = None,
    ) -> int:
        """
        Transfer all data from BigQuery to a specified PostgreSQL table.
        Used by atomic_swap to load data into the temp table.
        """
        try:
            total_start = time.perf_counter()
            client = self._get_bq_client()

            # Get table schema
            bq_table = client.get_table(self.bq_table_ref)
            schema = bq_table.schema

            # Pre-compute field info
            field_info = [
                {
                    "name": f.name,
                    "name_lower": f.name.lower(),
                    "type": f.field_type,
                    "needs_conversion": needs_json_conversion(f.field_type),
                    "is_binary": is_binary_type(f.field_type),
                }
                for f in schema
            ]

            # Pre-compute columns — needed for effective_batch_size calculation
            columns = [fi["name_lower"] for fi in field_info]
            column_str = ", ".join(f'"{c}"' for c in columns)

            # psycopg2 hard-caps at 65535 parameters per statement.
            # Derive a safe row-per-batch limit so we never exceed it.
            MAX_PG_PARAMS = 50_000  # conservative margin below the 65535 ceiling
            effective_batch_size = batch_size if batch_size is not None else max(50, MAX_PG_PARAMS // max(1, len(columns)))
            print(f"   📦 Batch size: {effective_batch_size} rows  ({len(columns)} cols × {effective_batch_size} = {len(columns) * effective_batch_size:,} params)")

            # Stream rows from BigQuery — avoids loading all rows into memory at once
            cols_str = ", ".join(f"`{f.name}`" for f in schema)
            query = f"SELECT {cols_str} FROM `{self.bq_table_ref}`"
            print(f"   📊 Streaming rows from BQ: {self.bq_table_ref}")

            bq_start = time.perf_counter()
            bq_result_iter = client.query(query).result()
            if bq_result_iter.total_rows is not None and bq_result_iter.total_rows == 0:
                print("   ℹ️  No rows in source table.")
                return 0
            if bq_result_iter.total_rows:
                print(f"   📊 Total rows: {bq_result_iter.total_rows:,}")

            rows_inserted = 0
            insert_time_total = 0.0

            with self.pg_engine.connect() as conn:
                batch: List[tuple] = []
                batch_num = 0

                for row in bq_result_iter:
                    row_dict = dict(row)
                    converted = tuple(
                        self._convert_field_value(
                            row_dict.get(fi["name"]),
                            fi["type"],
                            fi["needs_conversion"],
                            fi["is_binary"],
                        )
                        for fi in field_info
                    )
                    batch.append(converted)

                    if len(batch) >= effective_batch_size:
                        batch_num += 1
                        t0 = time.perf_counter()
                        self._insert_batch_to_table(
                            conn, target_table, column_str, columns, batch, max_retries=3
                        )
                        insert_time_total += time.perf_counter() - t0
                        rows_inserted += len(batch)
                        print(f"     ✅ Batch {batch_num}: {rows_inserted:,} rows inserted...")
                        batch = []

                if batch:
                    batch_num += 1
                    t0 = time.perf_counter()
                    self._insert_batch_to_table(
                        conn, target_table, column_str, columns, batch, max_retries=3
                    )
                    insert_time_total += time.perf_counter() - t0
                    rows_inserted += len(batch)
                    print(f"     ✅ Batch {batch_num} (final): {rows_inserted:,} rows")

                conn.connection.commit()

            total_elapsed = time.perf_counter() - bq_start
            total_time = time.perf_counter() - total_start
            print(f"   ⏱️  Load complete: BQ+PG streamed={total_elapsed:.2f}s | PG inserts={insert_time_total:.2f}s | Total={total_time:.2f}s")
            return rows_inserted

        except Exception as e:
            print(f"   ❌ Transfer to temp table failed: {str(e)}")
            raise

    def _insert_batch_to_table(
        self,
        conn,
        table_name: str,
        column_str: str,
        columns: List[str],
        batch: List[tuple],
        retry_count: int = 0,
        max_retries: int = 3,
    ):
        """Insert batch into a specified table (used for temp table in atomic swap)."""
        if not batch:
            return

        num_columns = len(columns)
        row_placeholders = ', '.join(['%s'] * num_columns)
        all_placeholders = ', '.join([f'({row_placeholders})' for _ in batch])

        # Escape % in column names so psycopg2 doesn't treat them as param placeholders
        safe_column_str = column_str.replace('%', '%%')

        insert_sql = f'INSERT INTO "{table_name}" ({safe_column_str}) VALUES {all_placeholders}'
        flat_values = [v for row in batch for v in row]

        try:
            raw_conn = conn.connection.connection
            cursor = raw_conn.cursor()
            cursor.execute(insert_sql, flat_values)
            cursor.close()
        except Exception as e:
            error_msg = str(e)[:150]
            is_transient = any(
                kw in error_msg.lower()
                for kw in ["connection", "timeout", "temporarily", "unavailable", "recovery", "deadlock"]
            )
            if is_transient and retry_count < max_retries:
                delay = 2 ** retry_count
                print(f"      ⚠️  Insert transient error (attempt {retry_count + 1}/{max_retries}), retrying in {delay}s...")
                time.sleep(delay)
                return self._insert_batch_to_table(
                    conn, table_name, column_str, columns, batch, retry_count + 1, max_retries
                )
            raise
