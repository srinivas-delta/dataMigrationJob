import traceback
import json
from typing import Dict, List, Tuple, Any
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
    
    def __init__(self, bq_table_ref: str, gcp_credentials: Any, pg_connection_string: str):
        self.bq_table_ref = bq_table_ref
        self.project, self.dataset, self.table = self._parse_table_ref(bq_table_ref)
        self.gcp_credentials = gcp_credentials
        
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
        """Create PostgreSQL table using DDL, with retry logic"""
        try:
            max_retries = 3
            for attempt in range(max_retries + 1):
                try:
                    with self.pg_engine.connect() as conn:
                        conn.execute(text(ddl))
                        conn.commit()
                    print(f"✅ Created table '{self.pg_table_name}'")
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
    
    def transfer_data(self, batch_size: int = 1000):

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
                    
                    # Insert batch when it reaches batch_size
                    if len(batch) >= batch_size:
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
                conn.commit()
            
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
        
        # Single INSERT statement with all rows
        insert_sql = f'INSERT INTO "{self.pg_table_name}" ({column_str}) VALUES {all_placeholders}'
        
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
                        single_sql = f'INSERT INTO "{self.pg_table_name}" ({column_str}) VALUES ({row_ph})'
                        
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

    
    def migrate(self, batch_size: int = 1000) -> Dict[str, Any]:
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
