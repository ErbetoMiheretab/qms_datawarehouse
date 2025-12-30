# src/core/database.py
import csv
import io
from typing import Dict
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import create_engine, text
from src.config import settings

import logging
# --- PostgreSQL Engine ---
logger = logging.getLogger("db")

try:
    engine = create_engine(
        settings.WAREHOUSE_URL,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
    )
    logger.info(f"PostgreSQL Engine created for {settings.WAREHOUSE_URL.split('@')[-1]}")
except Exception as e:
    logger.critical(f"Failed to create PostgreSQL engine: {e}")
    raise

# --- MongoDB Client Management ---
_mongo_clients: Dict[str, AsyncIOMotorClient] = {}

def get_mongo_client(uri: str) -> AsyncIOMotorClient:
    """Get or create a singleton MongoDB client for the given URI."""
    if uri not in _mongo_clients:
        _mongo_clients[uri] = AsyncIOMotorClient(uri)
    return _mongo_clients[uri]

def close_mongo_clients():
    """Close all active MongoDB connections."""
    for client in _mongo_clients.values():
        client.close()
    _mongo_clients.clear()

# --- Database Utilities ---
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

def init_history_table():
    """Ensures the sync history table exists."""
    query = text("""
        CREATE TABLE IF NOT EXISTS sync_history (
            id UUID PRIMARY KEY,
            source TEXT,
            collection TEXT,
            status TEXT,
            started_at TIMESTAMP WITH TIME ZONE,
            completed_at TIMESTAMP WITH TIME ZONE,
            records_synced INTEGER DEFAULT 0,
            message TEXT
        );
    """)
    with engine.begin() as conn:
        conn.execute(query)

def psql_insert_copy(table, conn, keys, data_iter):
    """
    SQLAlchemy `to_sql` method override.
    Executes SQL insertion using PostgreSQL COPY command for speed.
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(f'"{k}"' for k in keys)
        if table.schema:
            table_name = f'{table.schema}.{table.name}'
        else:
            table_name = table.name

        sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
        cur.copy_expert(sql=sql, file=s_buf)

async def run_sync(func, *args, **kwargs):
    """
    Runs a synchronous function in a thread pool to avoid blocking the event loop.
    Example: await run_sync(some_blocking_io, arg1, kwarg2='value')
    """
    import asyncio
    from functools import partial
    
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))