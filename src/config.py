# src/config.py
import json
import os

# Use built-in dict type for annotations (typing.Dict is deprecated)
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Postgres
    WAREHOUSE_URL = os.getenv("WAREHOUSE_URL")
    
    # Redis
    REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
    
    # App Config
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "source_db")
    SYNC_BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "5000"))
    
    # Auth
    API_KEY = os.getenv("API_KEY")
    API_KEY_NAME = "X-API-Key"

    # Mongo Sources Dictionary
    MONGO_SOURCES: dict[str, str] = {}

    def __init__(self):
        """
        Parses MONGO_SOURCES environment variable.
        Supports:
        1. JSON Dictionary: '{"prod": "mongo://a", "dev": "mongo://b"}'
        2. CSV List (Fallback): "mongo://a, mongo://b" -> Auto-names to source_1, source_2
        """
        sources_str = os.getenv("MONGO_SOURCES", "")
        
        try:
            # Attempt 1: Parse as JSON Key-Value pairs
            parsed = json.loads(sources_str)
            if isinstance(parsed, dict):
                self.MONGO_SOURCES = parsed
            else:
                # If JSON is valid but it's a list, trigger fallback
                raise ValueError("JSON content is not a dictionary")
        except (json.JSONDecodeError, ValueError):
            # Attempt 2: Fallback to Comma-Separated List
            raw_list = [uri.strip() for uri in sources_str.split(",") if uri.strip()]
            self.MONGO_SOURCES = {f"source_{i+1}": uri for i, uri in enumerate(raw_list)}

    def validate(self):
        if not self.WAREHOUSE_URL:
            raise RuntimeError("WAREHOUSE_URL environment variable is required")
        if not self.MONGO_SOURCES:
            raise RuntimeError("MONGO_SOURCES env var must be set (JSON dict or CSV list)")

# Initialize and validate
settings = Settings()
settings.validate()