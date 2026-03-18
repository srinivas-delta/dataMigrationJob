import os
import json
import base64
import glob
from typing import Dict, Any, Optional
from pathlib import Path
from google.oauth2 import service_account
from dotenv import load_dotenv

# Service account JSON must contain at least these keys to be valid
_REQUIRED_SA_KEYS = {"type", "project_id", "private_key_id", "client_email"}


def _is_valid_sa_json(data: dict) -> bool:
    return _REQUIRED_SA_KEYS.issubset(data.keys())


def _credentials_from_json(json_data: dict):
    """
    Build credentials and also set GOOGLE_APPLICATION_CREDENTIALS so that
    google.auth.default() / ADC works for ALL GCP libraries (BigQuery, Storage, etc.).
    This mirrors the pattern in vision-bi-backend/helpers/auth_loader.py.
    """
    os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"] = json.dumps(json_data)
    return service_account.Credentials.from_service_account_info(json_data)


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

        # 1 — Base64 env variable (AWS / Cloud Run / CI-CD — recommended for non-GCP hosts)
        b64_value = os.getenv("GCP_SERVICE_ACCOUNT_B64")
        if b64_value and b64_value.strip():
            try:
                decoded_json_str = base64.b64decode(b64_value).decode("utf-8")
                json_data = json.loads(decoded_json_str)
                if not _is_valid_sa_json(json_data):
                    raise ValueError("JSON does not look like a service account file")
                print("✅ Loaded GCP credentials from GCP_SERVICE_ACCOUNT_B64")
                return _credentials_from_json(json_data)
            except Exception as e:
                print(f"❌ Failed decoding GCP_SERVICE_ACCOUNT_B64: {e}")

        # 2 — Explicit JSON file path in env
        json_path = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        if json_path and os.path.exists(json_path):
            try:
                with open(json_path, "r") as f:
                    json_data = json.load(f)
                if not _is_valid_sa_json(json_data):
                    raise ValueError("JSON does not look like a service account file")
                print(f"✅ Loaded GCP credentials from {json_path}")
                return _credentials_from_json(json_data)
            except Exception as e:
                print(f"❌ Failed loading from {json_path}: {e}")

        # 3 — Auto-detect seismic-*.json in project root (development)
        search_paths = [Path("."), ConfigLoader._get_project_root()]
        for search_dir in search_paths:
            for filepath in search_dir.glob("seismic*.json"):
                try:
                    with open(filepath, "r") as f:
                        json_data = json.load(f)
                    if not _is_valid_sa_json(json_data):
                        continue
                    print(f"✅ Loaded GCP credentials from local file: {filepath.name}")
                    return _credentials_from_json(json_data)
                except Exception:
                    continue

        raise RuntimeError(
            "❌ No valid Google service account credentials found.\n"
            "Checked (in order):\n"
            "  1. GCP_SERVICE_ACCOUNT_B64   ← base64 JSON in .env (use on AWS/Cloud Run)\n"
            "  2. GCP_SERVICE_ACCOUNT_JSON  ← path to JSON file in .env\n"
            "  3. seismic-*.json            ← auto-detected in project root (dev only)\n\n"
            "To generate base64 for AWS:\n"
            "  cat seismic-helper-364013-da1f4f449e3b.json | base64 | tr -d '\\n'"
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
