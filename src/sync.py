import logging
import asyncio
import os
import pandas as pd
import io
import json
import csv
from datetime import datetime, timezone
from typing import Optional
from functools import partial
from sqlalchemy import text
from src.db import engine, get_mongo_client

logger = logging.getLogger("sync")

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Prepares dataframe for SQL insertion."""
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
    
    # Convert lists/dicts to JSON strings to allow Postgres JSONB ingestion
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
            
    return df

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data using PostgreSQL COPY command.
    Extremely fast for large datasets.
    """
    # gets a DBAPI connection can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def write_df_to_sql_sync(df: pd.DataFrame, table_name: str):
    """
    Synchronous function to write DataFrame to SQL using high-speed COPY.
    """
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False, method=psql_insert_copy)

async def get_last_synced(source_uri: str, collection_name: str) -> Optional[datetime]:
    query = text("""
        SELECT last_synced_at FROM sync_metadata
        WHERE source_uri = :source_uri AND collection_name = :collection_name
    """)
    # Running short SQL reads in the main thread is usually acceptable, 
    # but could also be offloaded if high concurrency is expected.
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

async def sync_collection_streaming(
    source_uri: str,
    collection_name: str,
    batch_size: int = 5000
):
    logger.info(f"Starting sync for {collection_name} from {source_uri}")
    client = get_mongo_client(source_uri)
    
    # Dynamically get database name from URI or fallback to MONGO_DB_NAME env var
    db_name = client.get_default_database().name if client.get_default_database() is not None else os.getenv("MONGO_DB_NAME", "source_db")
    db = client[db_name]

    last_synced = await get_last_synced(source_uri, collection_name)
    query = {}
    if last_synced:
        query["updated_at"] = {"$gt": last_synced}

    # Sort is critical for resumability
    cursor = db[collection_name].find(query).sort("updated_at", 1)
    
    buffer = []
    count = 0
    latest_timestamp = last_synced
    
    # Get the running event loop to schedule blocking tasks
    loop = asyncio.get_running_loop()

    try:
        async for doc in cursor:
            buffer.append(doc)
            
            # Track high-water mark
            updated_at = doc.get("updated_at")
            if updated_at:
                # Ensure we handle basic datetime objects
                if isinstance(updated_at, datetime):
                    if latest_timestamp is None or updated_at > latest_timestamp:
                        latest_timestamp = updated_at

            if len(buffer) >= batch_size:
                df = pd.json_normalize(buffer, sep="_")
                df = clean_dataframe(df)
                df["_source"] = source_uri
                df["_synced_at"] = datetime.now(timezone.utc)
                
                # CRITICAL: Run blocking I/O in thread pool
                await loop.run_in_executor(
                    None,  # Uses default ThreadPoolExecutor
                    partial(write_df_to_sql_sync, df, collection_name)
                )
                
                count += len(buffer)
                buffer = []

        # Process remaining buffer
        if buffer:
            df = pd.json_normalize(buffer, sep="_")
            df = clean_dataframe(df)
            df["_source"] = source_uri
            df["_synced_at"] = datetime.now(timezone.utc)
            
            await loop.run_in_executor(
                None,
                partial(write_df_to_sql_sync, df, collection_name)
            )
            count += len(buffer)

        if latest_timestamp:
            await update_last_synced(source_uri, collection_name, latest_timestamp)

        msg = f"Synced {count} rows from {source_uri} to {collection_name}"
        logger.info(msg)
        return msg

    except Exception as e:
        logger.exception(f"Failed to sync {collection_name} from {source_uri}: {e}")
        raise