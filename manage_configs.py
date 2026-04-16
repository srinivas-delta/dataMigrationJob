#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Migration Config Manager

Utility script to manage migration configurations stored in PostgreSQL.
Use this to seed configs from JSON, list configs, enable/disable migrations, etc.
"""

import sys
import argparse
from pathlib import Path
from src.config_loader import ConfigLoader
from src.config_table import ConfigTableManager


def main():
    parser = argparse.ArgumentParser(
        description='Manage migration configurations in PostgreSQL'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Seed command
    seed_parser = subparsers.add_parser('seed', help='Seed configs from JSON file')
    seed_parser.add_argument(
        'json_file',
        type=str,
        help='Path to JSON config file'
    )
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all configurations')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add a new migration config')
    add_parser.add_argument('--bq-table', required=True, help='BigQuery table (project.dataset.table)')
    add_parser.add_argument('--mode', default='full', help='Migration mode: full, safe_upsert, watermark_append, atomic_swap')
    add_parser.add_argument('--primary-key', help='Primary key column')
    add_parser.add_argument('--watermark-column', help='Watermark column for incremental sync')
    add_parser.add_argument('--batch-size', type=int, help='Batch size for data transfer')
    add_parser.add_argument('--description', help='Configuration description')
    
    # Enable command
    enable_parser = subparsers.add_parser('enable', help='Enable a migration config')
    enable_parser.add_argument('bq_table', help='BigQuery table to enable')
    
    # Disable command
    disable_parser = subparsers.add_parser('disable', help='Disable a migration config')
    disable_parser.add_argument('bq_table', help='BigQuery table to disable')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Load PostgreSQL config
    try:
        pg_config = ConfigLoader.load_postgres_config()
        pg_connection_string = ConfigLoader.build_postgres_connection_string(pg_config)
    except Exception as e:
        print(f"❌ Failed to load PostgreSQL config: {str(e)}")
        sys.exit(1)
    
    # Initialize config manager
    manager = ConfigTableManager(pg_connection_string)
    manager.ensure_table_exists()
    
    # Handle commands
    if args.command == 'seed':
        if not Path(args.json_file).exists():
            print(f"❌ JSON file not found: {args.json_file}")
            sys.exit(1)
        ConfigTableManager.seed_from_json(pg_connection_string, args.json_file)
        print("✅ Successfully seeded configurations from JSON")
    
    elif args.command == 'list':
        manager.list_configs()
    
    elif args.command == 'add':
        success = manager.add_config(
            bq_table=args.bq_table,
            mode=args.mode,
            primary_key=args.primary_key,
            watermark_column=args.watermark_column,
            batch_size=args.batch_size,
            description=args.description,
        )
        sys.exit(0 if success else 1)
    
    elif args.command == 'enable':
        success = manager.toggle_config(args.bq_table, enabled=True)
        sys.exit(0 if success else 1)
    
    elif args.command == 'disable':
        success = manager.toggle_config(args.bq_table, enabled=False)
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
