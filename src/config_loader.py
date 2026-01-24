import os
import json
import base64
from typing import Dict, Any, Optional
from pathlib import Path
from google.oauth2 import service_account
from dotenv import load_dotenv


class ConfigLoader:
    """Load and manage migration job configuration"""
    
    # Load .env file on class initialization
    _env_loaded = False
    
    @classmethod
    def _ensure_env_loaded(cls) -> None:
        """Load .env file once at startup"""
        if not cls._env_loaded:
            env_file = cls._get_project_root() / ".env"
            if env_file.exists():
                load_dotenv(str(env_file), override=False)
                print(f"✅ Loaded environment from {env_file}")
            cls._env_loaded = True
    
    @staticmethod
    def _get_project_root() -> Path:
        current_file = Path(__file__).resolve()
        # Go up from src/config_loader.py -> project root
        return current_file.parent.parent
    
    @staticmethod
    def load_gcp_credentials() -> Any:

        # Ensure .env is loaded
        ConfigLoader._ensure_env_loaded()
        
        # 1 — Try Base64 env variable
        b64_value = os.getenv("GCP_SERVICE_ACCOUNT_B64")
        if b64_value:
            try:
                decoded_json = base64.b64decode(b64_value).decode("utf-8")
                json_data = json.loads(decoded_json)
                print("✅ Loaded GCP credentials from GCP_SERVICE_ACCOUNT_B64")
                return service_account.Credentials.from_service_account_info(json_data)
            except Exception as e:
                print(f"❌ Failed decoding GCP_SERVICE_ACCOUNT_B64: {e}")
        
        # 2 — Try JSON file path
        json_path = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        if json_path and os.path.exists(json_path):
            try:
                print(f"✅ Loaded GCP credentials from {json_path}")
                return service_account.Credentials.from_service_account_file(json_path)
            except Exception as e:
                print(f"❌ Failed loading from {json_path}: {e}")
        
        # 3 — Try local seismic*.json files in project root (for dev)
        # Search in both current directory and project root
        search_paths = [Path("."), ConfigLoader._get_project_root()]
        for search_dir in search_paths:
            try:
                for filename in search_dir.glob("seismic*.json"):
                    try:
                        creds = service_account.Credentials.from_service_account_file(str(filename))
                        print(f"✅ Loaded GCP credentials from local file: {filename.name}")
                        return creds
                    except Exception:
                        pass
            except Exception:
                pass
        
        raise RuntimeError(
            "❌ No valid Google service account credentials found.\n"
            "Set one of: GCP_SERVICE_ACCOUNT_B64, GCP_SERVICE_ACCOUNT_JSON, or place seismic-*.json"
        )
    
    @staticmethod
    def load_postgres_config() -> Dict[str, Any]:
        # Ensure .env is loaded
        ConfigLoader._ensure_env_loaded()
        
        config = {
            'host': os.getenv("DB_HOST", "localhost"),
            'port': os.getenv("DB_PORT", "5432"),
            'name': os.getenv("DB_NAME", "postgres"),
            'user': os.getenv("DB_USER", "postgres"),
            'password': os.getenv("DB_PASSWORD", ""),
        }
        return config
    
    @staticmethod
    def build_postgres_connection_string(config: Dict[str, Any]) -> str:
        """Build PostgreSQL connection string from config"""
        return (
            f"postgresql+psycopg2://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['name']}"
        )
    
    @staticmethod
    def load_config_file(file_path: str) -> Dict[str, Any]:

        

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Config file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            config = json.load(f)
        
        print(f"✅ Loaded configuration from {file_path}")
        return config
