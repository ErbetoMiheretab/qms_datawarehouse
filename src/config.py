# src/config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Postgres
    WAREHOUSE_URL = os.getenv("WAREHOUSE_URL")
    
    # Mongo
    MONGO_SOURCES_STR = os.getenv("MONGO_SOURCES", "")
    MONGO_SOURCES = [uri.strip() for uri in MONGO_SOURCES_STR.split(",") if uri.strip()]
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "source_db")
    
    # Redis
    REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
    
    # App
    # API_KEY = os.getenv("API_KEY")
    API_KEY_NAME = "X-API-Key"
    SYNC_BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "5000"))

    def validate(self):
        if not self.WAREHOUSE_URL:
            raise RuntimeError("WAREHOUSE_URL environment variable is required")
        if not self.MONGO_SOURCES:
            raise RuntimeError("MONGO_SOURCES env var must be comma-separated MongoDB URIs")

settings = Settings()
settings.validate()