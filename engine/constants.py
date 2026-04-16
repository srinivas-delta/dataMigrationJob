"""
Centralized Configuration Module - Constants and Environment Setup
Mirrors the pattern from vision-pit-backend/engine/constants.py

This module loads all configuration at startup and provides singleton
access to credentials and database connections.

All clients and engines are created at import time for maximum performance:
  - GCP_CREDENTIALS: Service account credentials (singleton)
  - PG_ENGINE: SQLAlchemy PostgreSQL connection pool
  - PG_CONFIG: Database configuration dict
  - PG_CONNECTION_STRING: Raw connection string
"""

import os
import logging
from decouple import config, UndefinedValueError
from helpers.gcp_credentials import get_gcp_credentials
from dotenv import load_dotenv
from sqlalchemy import create_engine, pool

# Configure logging
logger = logging.getLogger(__name__)

# Load .env file at module import time (once)
_env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
if os.path.exists(_env_file):
    load_dotenv(_env_file, override=False)
    print(f"✅ Loaded environment from {_env_file}")


# ============================================================================
# GCP CONFIGURATION
# ============================================================================

try:
    GCP_PROJECT_ID = config("GCP_PROJECT_ID", default=None)
    GCP_CREDENTIALS = get_gcp_credentials()
    
    # Extract project ID from credentials if not set explicitly
    if not GCP_PROJECT_ID and hasattr(GCP_CREDENTIALS, "project_id"):
        GCP_PROJECT_ID = GCP_CREDENTIALS.project_id
    
    if GCP_PROJECT_ID:
        print(f"✅ GCP Project ID: {GCP_PROJECT_ID}")
except Exception as e:
    logger.error(f"❌ Failed to initialize GCP credentials: {e}")
    raise


# ============================================================================
# ENVIRONMENT CONFIGURATION
# ============================================================================

try:
    ENV = config("ENV", default="development")
    print(f"✅ Environment: {ENV}")
except Exception as e:
    logger.warning(f"ENV not set, defaulting to 'development': {e}")
    ENV = "development"


# ============================================================================
# POSTGRESQL CONFIGURATION
# ============================================================================

def get_postgres_config():
    """
    Load PostgreSQL configuration from environment variables.
    
    Supports both direct env vars and through decouple config.
    """
    return {
        "host": config("DB_HOST", default="localhost"),
        "port": int(config("DB_PORT", default=5432)),
        "name": config("DB_NAME", default="postgres"),
        "user": config("DB_USER", default="postgres"),
        "password": config("DB_PASSWORD", default=""),
    }


def build_postgres_connection_string(pg_config: dict = None) -> str:
    """
    Build SQLAlchemy PostgreSQL connection string.
    
    Args:
        pg_config: Dict with keys [host, port, name, user, password].
                  If None, loads from environment variables.
    
    Returns:
        PostgreSQL connection string for SQLAlchemy
    """
    if pg_config is None:
        pg_config = get_postgres_config()
    
    return (
        f"postgresql+psycopg2://{pg_config['user']}:{pg_config['password']}"
        f"@{pg_config['host']}:{pg_config['port']}/{pg_config['name']}"
    )


# Load and validate PostgreSQL config at startup
try:
    PG_CONFIG = get_postgres_config()
    PG_CONNECTION_STRING = build_postgres_connection_string(PG_CONFIG)
    
    print(f"✅ PostgreSQL Config: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['name']}")
    
    # Create persistent SQLAlchemy engine at startup (singleton)
    # Matches pattern from vision-pit-backend (they create bq_client, st_client at startup)
    PG_ENGINE = create_engine(
        PG_CONNECTION_STRING,
        poolclass=pool.QueuePool,
        pool_size=5,              # Keep 5 connections open
        max_overflow=10,          # Allow up to 10 overflow connections
        pool_pre_ping=True,       # Verify connection before using (handles stale connections)
        pool_recycle=3600,        # Recycle connections after 1 hour
        echo=False,               # Set to True for SQL query logging (debug only)
    )
    
    print(f"✅ PostgreSQL Engine: pool_size=5, max_overflow=10")
    
except Exception as e:
    logger.warning(f"PostgreSQL configuration incomplete: {e}")
    PG_CONFIG = get_postgres_config()
    PG_CONNECTION_STRING = build_postgres_connection_string(PG_CONFIG)
    
    # Still create engine even if partial config (might fail at runtime)
    try:
        PG_ENGINE = create_engine(
            PG_CONNECTION_STRING,
            poolclass=pool.QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    except Exception as engine_error:
        logger.error(f"Failed to create PostgreSQL engine: {engine_error}")
        PG_ENGINE = None


# ============================================================================
# BIGQUERY CONFIGURATION
# ============================================================================

try:
    BQ_PROJECT = config("BQ_PROJECT", default=GCP_PROJECT_ID)
    BQ_DATASET = config("BQ_DATASET", default="")
except Exception as e:
    logger.warning(f"BigQuery configuration: {e}")
    BQ_PROJECT = GCP_PROJECT_ID
    BQ_DATASET = ""


# ============================================================================
# MIGRATION JOB CONFIGURATION
# ============================================================================

try:
    PARALLEL_WORKERS = int(config("PARALLEL_WORKERS", default=1))
    BATCH_SIZE = int(config("BATCH_SIZE", default=1000))
    ENABLE_VALIDATION = config("ENABLE_VALIDATION", default="True").lower() == "true"
    ENABLE_SCHEMA_DRIFT = config("ENABLE_SCHEMA_DRIFT", default="True").lower() == "true"
except Exception as e:
    logger.warning(f"Migration configuration defaults applied: {e}")
    PARALLEL_WORKERS = 1
    BATCH_SIZE = 1000
    ENABLE_VALIDATION = True
    ENABLE_SCHEMA_DRIFT = True


print(f"✅ Migration Config: workers={PARALLEL_WORKERS}, batch_size={BATCH_SIZE}")
