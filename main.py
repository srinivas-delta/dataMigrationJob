#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BigQuery to PostgreSQL Migration Job - Main Entry Point

This is a standalone migration tool that can be used to migrate tables from BigQuery to PostgreSQL.

Usage:
    python main.py                          # Uses config/tables.json
    python main.py --config config/tables.json
    python main.py --table project.dataset.table
    python main.py --batch-size 500
"""

import sys
import argparse
import json
from datetime import datetime
from pathlib import Path

# Force UTF-8 output on Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from src.config_loader import ConfigLoader
from src.bq_to_postgres_migrator import BQToPostgresMigrator


def safe_print(text):
    """Print text with fallback for emoji encoding issues"""
    try:
        print(text)
    except UnicodeEncodeError:
        # Fallback: replace emoji with brackets
        fallback = (text
            .replace('🔐', '[LOCK]')
            .replace('✅', '[OK]')
            .replace('❌', '[ERROR]')
            .replace('📊', '[DATA]')
            .replace('🗄️', '[DB]')
            .replace('🔧', '[CONFIG]')
            .replace('🗑️', '[DELETE]')
            .replace('🏗️', '[BUILD]')
            .replace('📦', '[PACKAGE]')
            .replace('📋', '[LIST]')
            .replace('🎯', '[TARGET]')
        )
        print(fallback)


def main():
    parser = argparse.ArgumentParser(
        description='Migrate tables from BigQuery to PostgreSQL'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/tables.json',
        help='Path to config file with tables to migrate (default: config/tables.json)'
    )
    parser.add_argument(
        '--table',
        type=str,
        help='Single table to migrate (format: project.dataset.table)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for data transfer (default: 1000)'
    )
    
    args = parser.parse_args()
    
    print(f"\n{'#'*60}")
    print(f"# BigQuery to PostgreSQL Migration Job")
    print(f"# Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}\n")
    
    try:
        # Load credentials and config
        print("🔐 Loading GCP credentials...")
        gcp_credentials = ConfigLoader.load_gcp_credentials()
        
        print("\n🗄️  Loading PostgreSQL configuration...")
        pg_config = ConfigLoader.load_postgres_config()
        pg_connection_string = ConfigLoader.build_postgres_connection_string(pg_config)
        print(f"   Host: {pg_config['host']}:{pg_config['port']}")
        print(f"   Database: {pg_config['name']}")
        
        tables_to_migrate = []
        
        # Single table mode
        if args.table:
            tables_to_migrate = [args.table]
            print(f"\n📋 Single table mode: {args.table}")
        
        # Config file mode
        else:
            if not Path(args.config).exists():
                print(f"❌ Config file not found: {args.config}")
                print(f"   Create {args.config} or use --table parameter")
                sys.exit(1)
            
            config = ConfigLoader.load_config_file(args.config)
            tables_to_migrate = [item['bq_table'] for item in config.get('migrations', [])]
            batch_size = config.get('batch_size', args.batch_size)
        
        if not tables_to_migrate:
            print("❌ No tables to migrate. Exiting.")
            sys.exit(1)
        
        print(f"\n📊 Tables to migrate: {len(tables_to_migrate)}")
        for table in tables_to_migrate:
            print(f"   - {table}")
        
        # Perform migrations
        results = []
        for i, table_ref in enumerate(tables_to_migrate, 1):
            print(f"\n[{i}/{len(tables_to_migrate)}] Processing: {table_ref}")
            
            try:
                migrator = BQToPostgresMigrator(
                    bq_table_ref=table_ref,
                    gcp_credentials=gcp_credentials,
                    pg_connection_string=pg_connection_string
                )
                result = migrator.migrate(batch_size=args.batch_size)
                results.append(result)
            except Exception as e:
                print(f"❌ Failed to migrate {table_ref}: {str(e)}")
                results.append({
                    'success': False,
                    'bq_table': table_ref,
                    'error': str(e)
                })
        
        # Summary
        print(f"\n{'#'*60}")
        print(f"# Migration Summary")
        print(f"{'#'*60}")
        
        successful = [r for r in results if r.get('success')]
        failed = [r for r in results if not r.get('success')]
        
        safe_print(f"\n[OK] Successful: {len(successful)}/{len(results)}")
        for r in successful:
            rows = r.get('rows_transferred', 0)
            duration = r.get('duration_seconds', 0)
            print(f"   - {r['bq_table']} → {r['pg_table']} ({rows} rows in {duration:.2f}s)")
        
        if failed:
            safe_print(f"\n[ERROR] Failed: {len(failed)}/{len(results)}")
            for r in failed:
                safe_print(f"   - {r['bq_table']}: {r.get('error', 'Unknown error')}")
        
        safe_print(f"\n{'#'*60}")
        safe_print(f"# Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        safe_print(f"{'#'*60}\n")
        
        # Exit with appropriate code
        sys.exit(0 if len(failed) == 0 else 1)
        
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
