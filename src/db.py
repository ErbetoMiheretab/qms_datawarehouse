# src/db.py
import os

from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import create_engine, text

# --- PostgreSQL ---
WAREHOUSE_URL = os.getenv("WAREHOUSE_URL")
if not WAREHOUSE_URL:
    raise RuntimeError("WAREHOUSE_URL environment variable is required")

# Using pool_pre_ping to handle dropped connections automatically
engine = create_engine(
    WAREHOUSE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

def init_metadata_table():
    """Ensures the sync tracking table exists in Postgres."""
    query = text("""
        CREATE TABLE IF NOT EXISTS sync_metadata (
            source_uri TEXT,
            collection_name TEXT,
            last_synced_at TIMESTAMP WITH TIME ZONE,
            PRIMARY KEY (source_uri, collection_name)
        );
    """)
    with engine.begin() as conn:
        conn.execute(query)

# --- MongoDB ---
_mongo_clients: dict[str, AsyncIOMotorClient] = {}

def get_mongo_client(uri: str) -> AsyncIOMotorClient:
    """Get or create a singleton MongoDB client for the given URI."""
    if uri not in _mongo_clients:
        # motor clients are non-blocking and safe to share
        _mongo_clients[uri] = AsyncIOMotorClient(uri)
    return _mongo_clients[uri]

def close_mongo_clients():
    """Close all active MongoDB connections."""
    for client in _mongo_clients.values():
        client.close()
    _mongo_clients.clear()