# src/services/etl.py
import logging
import asyncio
import pandas as pd
from datetime import datetime, timezone
from functools import partial
from typing import Optional
from sqlalchemy import text

from src.config import settings
from src.core.db import engine, get_mongo_client, psql_insert_copy
from src.services.transform import clean_dataframe

logger = logging.getLogger("etl")

def write_df_to_sql_sync(df: pd.DataFrame, table_name: str):
    """
    Synchronous function to write DataFrame to SQL. 
    MUST run in executor.
    """
    with engine.begin() as conn:
        df.to_sql(
            table_name, 
            conn, 
            if_exists="append", 
            index=False, 
            method=psql_insert_copy
        )

async def get_last_synced(source_uri: str, collection_name: str) -> Optional[datetime]:
    query = text("""
        SELECT last_synced_at FROM sync_metadata
        WHERE source_uri = :source_uri AND collection_name = :collection_name
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"source_uri": source_uri, "collection_name": collection_name}).fetchone()
        if row and row[0]:
            ts = row[0]
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return None

async def update_last_synced(source_uri: str, collection_name: str, ts: datetime):
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
        
    query = text("""
        INSERT INTO sync_metadata (source_uri, collection_name, last_synced_at)
        VALUES (:source_uri, :collection_name, :last_synced_at)
        ON CONFLICT (source_uri, collection_name)
        DO UPDATE SET last_synced_at = EXCLUDED.last_synced_at
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "source_uri": source_uri,
            "collection_name": collection_name,
            "last_synced_at": ts
        })

async def sync_collection_streaming(source_uri: str, collection_name: str):
    batch_size = settings.SYNC_BATCH_SIZE
    logger.info(f"Starting sync for {collection_name} from {source_uri}")
    
    client = get_mongo_client(source_uri)
    try:
        default_db = client.get_default_database()
        db = default_db
    except Exception:
        db = client[settings.MONGO_DB_NAME]

    last_synced = await get_last_synced(source_uri, collection_name)
    query = {}
    if last_synced:
        query["updated_at"] = {"$gt": last_synced}

    cursor = db[collection_name].find(query).sort("updated_at", 1)
    
    buffer = []
    count = 0
    latest_timestamp = last_synced
    loop = asyncio.get_running_loop()

    try:
        async for doc in cursor:
            buffer.append(doc)
            
            # High-water mark tracking
            updated_at = doc.get("updated_at")
            if isinstance(updated_at, datetime):
                if latest_timestamp is None or updated_at > latest_timestamp:
                    latest_timestamp = updated_at

            if len(buffer) >= batch_size:
                await _process_batch(buffer, source_uri, collection_name, loop)
                count += len(buffer)
                buffer = []

        if buffer:
            await _process_batch(buffer, source_uri, collection_name, loop)
            count += len(buffer)

        if latest_timestamp:
            await update_last_synced(source_uri, collection_name, latest_timestamp)

        msg = f"Synced {count} rows from {source_uri} to {collection_name}"
        logger.info(msg)
        return msg

    except Exception as e:
        logger.exception(f"Failed to sync {collection_name} from {source_uri}: {e}")
        raise

async def _process_batch(buffer, source_uri, collection, loop):
    """Helper to process and write a batch in a separate thread."""
    df = pd.DataFrame(buffer)
    df = clean_dataframe(df)
    df["_source"] = source_uri
    df["_synced_at"] = datetime.now(timezone.utc)
    
    await loop.run_in_executor(
        None,
        partial(write_df_to_sql_sync, df, collection)
    )