"""
GCP Credentials Helper - Singleton Pattern
Mirrors the pattern from vision-pit-backend/helpers/gcp_credentials.py
"""

import os
import json
import base64
from google.oauth2 import service_account

# A shared global instance so credentials are loaded only once
GCP_CREDENTIALS = None


def get_gcp_credentials():
    """
    Returns a singleton service account credential object.
    Loads from Base64 env variable (AWS/Elastic Beanstalk) OR local json (dev mode).
    
    Fallback order:
    1. GCP_SERVICE_ACCOUNT_B64 (base64-encoded JSON) - AWS/Cloud Run
    2. GCP_SERVICE_ACCOUNT_JSON (file path) - GCP VMs/servers
    3. seismic*.json (auto-detected local file) - Development
    """
    global GCP_CREDENTIALS

    # If already loaded, reuse it (singleton pattern)
    if GCP_CREDENTIALS:
        return GCP_CREDENTIALS

    # 1 — Try Base64 env variable (AWS/Elastic Beanstalk/Cloud Run)
    b64_value = os.getenv("GCP_SERVICE_ACCOUNT_B64")

    if b64_value and b64_value.strip():
        try:
            decoded_json = base64.b64decode(b64_value).decode("utf-8")
            json_data = json.loads(decoded_json)

            GCP_CREDENTIALS = service_account.Credentials.from_service_account_info(json_data)

            print("✅ Loaded GCP SA credentials from Base64 env var (GCP_SERVICE_ACCOUNT_B64)")
            return GCP_CREDENTIALS

        except Exception as e:
            print(f"❌ Failed decoding GCP_SERVICE_ACCOUNT_B64: {e}")

    # 2 — Try explicit JSON file path (GCP VMs/servers with secrets mounted)
    json_path = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    
    if json_path and os.path.exists(json_path):
        try:
            GCP_CREDENTIALS = service_account.Credentials.from_service_account_file(json_path)
            print(f"✅ Loaded GCP SA credentials from file: {json_path}")
            return GCP_CREDENTIALS
        except Exception as e:
            print(f"❌ Failed loading GCP credentials from {json_path}: {e}")

    # 3 — Auto-detect local seismic*.json (dev mode)
    for filename in os.listdir("."):
        if filename.startswith("seismic") and filename.endswith(".json"):
            try:
                GCP_CREDENTIALS = service_account.Credentials.from_service_account_file(filename)
                print(f"✅ Loaded GCP SA credentials from local file: {filename}")
                return GCP_CREDENTIALS
            except Exception as e:
                print(f"⚠️  Skipped {filename}: {e}")
                continue

    # No credentials found
    raise RuntimeError(
        "❌ No valid Google service account credentials found.\n"
        "Fallback order checked:\n"
        "  1. GCP_SERVICE_ACCOUNT_B64    ← base64-encoded JSON (AWS/Cloud Run)\n"
        "  2. GCP_SERVICE_ACCOUNT_JSON   ← file path to JSON (GCP servers)\n"
        "  3. seismic*.json              ← auto-detected local file (dev)\n\n"
        "To create GCP_SERVICE_ACCOUNT_B64 from your JSON:\n"
        "  cat seismic-helper-364013-da1f4f449e3b.json | base64 | tr -d '\\n'\n"
        "Then add to .env as:\n"
        "  GCP_SERVICE_ACCOUNT_B64=<paste base64 string>"
    )
