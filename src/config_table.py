"""
Configuration Table Manager
============================
Manages migration configurations stored in PostgreSQL table.
Allows dynamic configuration without redeploying JSON files.
"""

from typing import Dict, List, Any, Optional
import json
from sqlalchemy import create_engine, text, Table, Column, String, Integer, Boolean, MetaData, JSON
from sqlalchemy.orm import sessionmaker


class ConfigTableManager:
    """Manages migration configs stored in PostgreSQL table."""
    
    # Table schema
    TABLE_NAME = "migration_configs"
    SCHEMA = {
        "id": "INT PRIMARY KEY AUTO_INCREMENT",
        "bq_table": "VARCHAR(255) NOT NULL UNIQUE",
        "mode": "VARCHAR(50) DEFAULT 'full'",
        "primary_key": "VARCHAR(100)",
        "watermark_column": "VARCHAR(100)",
        "schema_drift": "JSON DEFAULT '{}'",
        "enabled": "BOOLEAN DEFAULT true",
        "batch_size": "INT",
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "description": "TEXT",
    }
    
    def __init__(self, pg_connection_string: str):
        """Initialize with PostgreSQL connection string."""
        self.pg_connection_string = pg_connection_string
        self.engine = create_engine(pg_connection_string, pool_pre_ping=True)
    
    def ensure_table_exists(self) -> None:
        """Create migration_configs table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            bq_table VARCHAR(255) NOT NULL UNIQUE,
            mode VARCHAR(50) DEFAULT 'full',
            primary_key VARCHAR(100),
            watermark_column VARCHAR(100),
            schema_drift JSONB DEFAULT '{{}}',
            enabled BOOLEAN DEFAULT true,
            batch_size INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        );
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
                print(f"✅ Configuration table '{self.TABLE_NAME}' ready")
        except Exception as e:
            print(f"⚠️  Table creation note: {str(e)[:100]}")
    
    def load_configs(self) -> Dict[str, Any]:
        """
        Load all enabled migration configurations from the table.
        
        Returns
        -------
        dict
            Dictionary with 'migrations' list and 'batch_size' key
        """
        configs = {'migrations': [], 'batch_size': None}
        
        query = f"""
        SELECT 
            bq_table,
            mode,
            primary_key,
            watermark_column,
            schema_drift,
            batch_size
        FROM {self.TABLE_NAME}
        WHERE enabled = true
        ORDER BY id
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                
                for row in rows:
                    # Parse schema_drift JSON if it's a string
                    schema_drift = row[4] if row[4] else {}
                    if isinstance(schema_drift, str):
                        schema_drift = json.loads(schema_drift)
                    
                    config = {
                        'bq_table': row[0],
                        'mode': row[1] or 'full',
                        'primary_key': row[2],
                        'watermark_column': row[3],
                        'schema_drift': schema_drift,
                    }
                    configs['migrations'].append(config)
                
                # Load batch_size from first row (global setting)
                if rows:
                    configs['batch_size'] = rows[0][5]
                
                print(f"✅ Loaded {len(configs['migrations'])} migration config(s) from database")
                return configs
        
        except Exception as e:
            print(f"❌ Error loading configs from database: {str(e)}")
            return configs
    
    def add_config(
        self,
        bq_table: str,
        mode: str = 'full',
        primary_key: Optional[str] = None,
        watermark_column: Optional[str] = None,
        schema_drift: Optional[Dict] = None,
        batch_size: Optional[int] = None,
        description: Optional[str] = None,
    ) -> bool:
        """Add a new migration configuration."""
        schema_drift = schema_drift or {'removed_policy': 'keep'}
        
        insert_sql = f"""
        INSERT INTO {self.TABLE_NAME} 
        (bq_table, mode, primary_key, watermark_column, schema_drift, batch_size, description)
        VALUES 
        (:bq_table, :mode, :primary_key, :watermark_column, :schema_drift::jsonb, :batch_size, :description)
        ON CONFLICT (bq_table) DO UPDATE SET
            mode = EXCLUDED.mode,
            primary_key = EXCLUDED.primary_key,
            watermark_column = EXCLUDED.watermark_column,
            schema_drift = EXCLUDED.schema_drift,
            batch_size = EXCLUDED.batch_size,
            description = EXCLUDED.description
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(insert_sql),
                    {
                        'bq_table': bq_table,
                        'mode': mode,
                        'primary_key': primary_key,
                        'watermark_column': watermark_column,
                        'schema_drift': json.dumps(schema_drift),
                        'batch_size': batch_size,
                        'description': description,
                    }
                )
                conn.commit()
                print(f"✅ Added/updated config for {bq_table}")
                return True
        except Exception as e:
            print(f"❌ Error adding config: {str(e)}")
            return False
    
    def toggle_config(self, bq_table: str, enabled: bool) -> bool:
        """Enable or disable a migration configuration."""
        update_sql = f"""
        UPDATE {self.TABLE_NAME}
        SET enabled = :enabled
        WHERE bq_table = :bq_table
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(update_sql),
                    {'enabled': enabled, 'bq_table': bq_table}
                )
                conn.commit()
                status = "enabled" if enabled else "disabled"
                print(f"✅ Config {status}: {bq_table}")
                return True
        except Exception as e:
            print(f"❌ Error toggling config: {str(e)}")
            return False
    
    def list_configs(self) -> None:
        """Display all configurations."""
        query = f"""
        SELECT id, bq_table, mode, enabled, primary_key, watermark_column, description
        FROM {self.TABLE_NAME}
        ORDER BY id
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                
                if not rows:
                    print(f"No configurations found in {self.TABLE_NAME}")
                    return
                
                print(f"\n{'ID':<4} {'Enabled':<8} {'BigQuery Table':<50} {'Mode':<15} {'PK':<15} {'Watermark':<30}")
                print("=" * 130)
                
                for row in rows:
                    status = "✓" if row[3] else "✗"
                    print(f"{row[0]:<4} {status:<8} {row[1]:<50} {row[2]:<15} {str(row[4]):<15} {str(row[5]):<30}")
        
        except Exception as e:
            print(f"❌ Error listing configs: {str(e)}")
    
    @staticmethod
    def seed_from_json(pg_connection_string: str, json_file: str) -> None:
        """
        Seed the configuration table from a JSON file.
        Useful for initial migration from JSON config to database config.
        """
        try:
            with open(json_file, 'r') as f:
                config = json.load(f)
            
            manager = ConfigTableManager(pg_connection_string)
            manager.ensure_table_exists()
            
            batch_size = config.get('batch_size')
            migrations = config.get('migrations', [])
            
            if not migrations:
                print(f"No migrations found in {json_file}")
                return
            
            print(f"\n🔄 Seeding {len(migrations)} migration(s) from {json_file}...")
            
            for migration in migrations:
                manager.add_config(
                    bq_table=migration['bq_table'],
                    mode=migration.get('mode', 'full'),
                    primary_key=migration.get('primary_key'),
                    watermark_column=migration.get('watermark_column'),
                    schema_drift=migration.get('schema_drift', {'removed_policy': 'keep'}),
                    batch_size=batch_size,
                )
            
            print(f"✅ Seeded {len(migrations)} configuration(s)")
        
        except Exception as e:
            print(f"❌ Error seeding configs: {str(e)}")
