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
from datetime import datetime, timezone
from pathlib import Path

# Force UTF-8 output on Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from sqlalchemy import create_engine
from src.config_loader import ConfigLoader
from src.bq_to_postgres_migrator import BQToPostgresMigrator
from src.watermark_store import WatermarkStore
from src.logger import get_logger, log_run_summary


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
    log = get_logger("main")

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
    
    log.info("=" * 60)
    log.info("BigQuery → PostgreSQL Migration Job started at %s",
             datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    log.info("=" * 60)
    
    try:
        # Load credentials and config
        log.info("Loading GCP credentials...")
        gcp_credentials = ConfigLoader.load_gcp_credentials()

        log.info("Loading PostgreSQL configuration...")
        pg_config = ConfigLoader.load_postgres_config()
        pg_connection_string = ConfigLoader.build_postgres_connection_string(pg_config)
        log.info("  Host: %s:%s  DB: %s", pg_config['host'], pg_config['port'], pg_config['name'])

        # Bootstrap watermark store (auto-creates sync_watermarks + sync_logs)
        pg_engine = create_engine(pg_connection_string, pool_pre_ping=True)
        store = WatermarkStore(pg_engine)
        store.ensure_tables()
        log.info("Sync log tables ready (sync_watermarks, sync_logs)")

        # Load migration configs
        if args.table:
            migration_configs = [{'bq_table': args.table}]
            print(f"\n📋 Single table mode: {args.table}")
        else:
            if not Path(args.config).exists():
                print(f"❌ Config file not found: {args.config}")
                print(f"   Create {args.config} or use --table parameter")
                sys.exit(1)

            config = ConfigLoader.load_config_file(args.config)
            migration_configs = config.get('migrations', [])
            args.batch_size = config.get('batch_size', args.batch_size)

        if not migration_configs:
            log.error("No tables to migrate — exiting.")
            sys.exit(1)

        log.info("Tables to migrate: %d", len(migration_configs))
        for item in migration_configs:
            log.info("  - %s  [mode=%s]", item['bq_table'], item.get('mode', 'full'))

        # Perform migrations
        results = []
        migration_configs = migration_configs if not args.table else [{'bq_table': args.table}]

        for i, table_cfg in enumerate(migration_configs, 1):
            table_ref = table_cfg['bq_table']
            short_name = table_ref.split('.')[-1].lower()
            mode = table_cfg.get('mode', 'full')   # 'safe_upsert' | 'full'
            primary_key = table_cfg.get('primary_key', 'id')
            watermark_col = table_cfg.get('watermark_column')
            drift_cfg = table_cfg.get('schema_drift', {})
            removed_policy = drift_cfg.get('removed_policy', 'keep')

            log.info("[%d/%d] Starting: %s  (mode=%s)", i, len(migration_configs), table_ref, mode)

            started_at = datetime.now(timezone.utc)
            log_id = store.log_start(short_name, table_ref)
            store.set_running(short_name, table_ref)

            try:
                migrator = BQToPostgresMigrator(
                    bq_table_ref=table_ref,
                    gcp_credentials=gcp_credentials,
                    pg_connection_string=pg_connection_string,
                    primary_key=primary_key,
                    removed_policy=removed_policy,
                )

                if mode == 'safe_upsert':
                    watermark_value = store.get_watermark(short_name)
                    result = migrator.migrate_safe(
                        batch_size=args.batch_size,
                        watermark_col=watermark_col,
                        watermark_value=watermark_value,
                    )
                elif mode == 'watermark_append':
                    watermark_value = store.get_watermark(short_name)
                    result = migrator.migrate_append(
                        batch_size=args.batch_size,
                        watermark_col=watermark_col,
                        watermark_value=watermark_value,
                    )
                else:
                    result = migrator.migrate(batch_size=args.batch_size)

                finished_at = datetime.now(timezone.utc)
                rows = result.get('rows_upserted', result.get('rows_transferred', 0))
                duration = (finished_at - started_at).total_seconds()

                store.set_success(short_name, table_ref, finished_at, rows)
                store.log_finish(
                    log_id,
                    status='success',
                    rows_affected=rows,
                    duration_secs=duration,
                    columns_added=result.get('columns_added', 0),
                    columns_dropped=result.get('columns_dropped_policy', 0),
                    type_mismatches=result.get('type_mismatches', 0),
                    drift_summary=result.get('drift_summary'),
                )
                log.info("[%s] SUCCESS | rows=%s | duration=%.2fs | mode=%s",
                         short_name, f"{rows:,}", duration, mode)
                results.append(result)

            except Exception as e:
                finished_at = datetime.now(timezone.utc)
                duration = (finished_at - started_at).total_seconds()
                err_msg = str(e)

                store.set_failed(short_name, table_ref, err_msg)
                store.log_finish(
                    log_id,
                    status='failed',
                    duration_secs=duration,
                    error=err_msg,
                )
                log.error("[%s] FAILED | duration=%.2fs | error=%s", short_name, duration, err_msg)
                results.append({
                    'success': False,
                    'bq_table': table_ref,
                    'error': err_msg
                })
        
        # Summary — written to both console and log file
        log_run_summary(log, results, triggered_by="cli")
        log.info("DB run history: SELECT * FROM sync_logs ORDER BY started_at DESC;")
        log.info("DB status     : SELECT * FROM sync_watermarks;")
        log.info("Completed at  : %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        successful = [r for r in results if r.get('success')]
        failed = [r for r in results if not r.get('success')]

        # Exit with appropriate code
        sys.exit(0 if len(failed) == 0 else 1)

    except Exception as e:
        log.exception("Fatal error: %s", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
