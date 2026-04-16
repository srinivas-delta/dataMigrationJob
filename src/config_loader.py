"""
Configuration Loader - Delegates to centralized engine/constants.py

This module provides backward compatibility while using the new
centralized configuration from engine/constants.py.

For new code, import directly from engine.constants instead:
  from engine.constants import GCP_CREDENTIALS, PG_CONFIG, PG_CONNECTION_STRING
"""

import os
import json
import glob
from typing import Dict, Any, Optional
from pathlib import Path
from helpers.gcp_credentials import get_gcp_credentials
from engine.constants import (
    PG_CONFIG,
    PG_CONNECTION_STRING,
    GCP_CREDENTIALS,
    GCP_PROJECT_ID,
    ENV,
)


class ConfigLoader:
    """
    Configuration Loader - DEPRECATED
    
    Use engine.constants instead for new code:
      from engine.constants import GCP_CREDENTIALS, PG_CONFIG, PG_CONNECTION_STRING
    
    This class is kept for backward compatibility with existing code.
    """
    
    @staticmethod
    def load_gcp_credentials() -> Any:
        """
        Load GCP service account credentials.
        
        Delegates to helpers.gcp_credentials.get_gcp_credentials() which uses
        the singleton pattern and supports multiple fallback methods:
          1. GCP_SERVICE_ACCOUNT_B64 (base64 JSON)
          2. GCP_SERVICE_ACCOUNT_JSON (file path)
          3. seismic-*.json (auto-detected local file)
        """
        return get_gcp_credentials()
    
    @staticmethod
    def load_postgres_config() -> Dict[str, Any]:
        """
        Load PostgreSQL configuration from environment variables.
        
        Delegates to engine.constants.get_postgres_config()
        """
        from engine.constants import get_postgres_config
        return get_postgres_config()
    
    @staticmethod
    def build_postgres_connection_string(config: Dict[str, Any] = None) -> str:
        """
        Build PostgreSQL connection string from config.
        
        Delegates to engine.constants.build_postgres_connection_string()
        """
        from engine.constants import build_postgres_connection_string
        return build_postgres_connection_string(config)
    
    @staticmethod
    def load_config_file(file_path: str) -> Dict[str, Any]:

        

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Config file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            config = json.load(f)
        
        print(f"✅ Loaded configuration from {file_path}")
        return config
