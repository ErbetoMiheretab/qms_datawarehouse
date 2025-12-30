import asyncio
import logging
import uuid
from datetime import UTC, datetime
from functools import partial
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.config import settings
from src.core.db import engine, get_mongo_client, psql_insert_copy, run_sync
from src.services.transform import clean_dataframe

logger = logging.getLogger("etl")

def write_df_to_sql_sync(df: pd.DataFrame, table_name: str):
    """
    Writes data using a Staging Table strategy to ensure Idempotency.
    1. Uploads to temp_table
    2. Inserts from temp_table to real_table (ON CONFLICT DO NOTHING)
    """
    if df.empty:
        return
    # Create a unique temp table name
    temp_table_name = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"

    # Check if target table exists
    from sqlalchemy import inspect
    
    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table(table_name):
            logger.info(f"Table '{table_name}' does not exist. Creating it...")
            # Create table directly
            df.to_sql(
                table_name, 
                conn, 
                if_exists="replace", # Create fresh
                index=False,
                method=psql_insert_copy
            )
            # Add Primary Key for future upserts
            try:
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("_id")'))
                logger.info(f"Added PRIMARY KEY constraint to '{table_name}'")
            except Exception as e:
                logger.warning(f"Could not add PK to {table_name}: {e}")
            return

        # If table exists, proceed with Staging Upsert Strategy
        df.to_sql(
            temp_table_name, 
            conn, 
            if_exists="replace", 
            index=False, 
            method=psql_insert_copy
        )

        columns = list(df.columns)
        cols_str = ", ".join([f'"{c}"' for c in columns])

        # Generates: "name" = EXCLUDED."name", "age" = EXCLUDED."age"
        # We exclude '_id' from the SET clause because it's the unique key
        update_set_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in columns if c != "_id"])

        
        sql = text(f"""
            INSERT INTO "{table_name}" ({cols_str})
            SELECT {cols_str}
            FROM "{temp_table_name}"
            ON CONFLICT (_id) 
            DO UPDATE SET
            {update_set_str};
        """)
        
        try:
            conn.execute(sql)
        except Exception as e:
            # If the table doesn't exist yet or constraint is missing, fallback to simple append
            # But warn the user they need a Unique Constraint for this to work perfectly.
            logger.error(f"Upsert failed. Ensure '{table_name}' has a UNIQUE constraint on '_id'. Error: {e}")
            # df.to_sql(table_name, conn, if_exists="append", index=False, method=psql_insert_copy)
            raise e # Fail hard so we don't skip data silently
        
        # 4. Clean up temp table
        conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table_name}"'))

def _get_last_synced_sync(source_name: str, collection_name: str) -> datetime | None:
    """Sync implementation of get_last_synced."""
    query = text("""
        SELECT last_synced_at FROM sync_metadata
        WHERE source_uri = :source_name AND collection_name = :collection_name
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"source_name": source_name, "collection_name": collection_name}).fetchone()
        if row and row[0]:
            ts = row[0]
            return ts if ts.tzinfo else ts.replace(tzinfo=UTC)
    return None

async def get_last_synced(source_name: str, collection_name: str) -> datetime | None:
    """
    Fetches the last sync timestamp using the Source Name (e.g., 'primary_site').
    """
    return await run_sync(_get_last_synced_sync, source_name, collection_name)

def _update_last_synced_sync(source_name: str, collection_name: str, ts: datetime):
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
        
    query = text("""
        INSERT INTO sync_metadata (source_uri, collection_name, last_synced_at)
        VALUES (:source_name, :collection_name, :last_synced_at)
        ON CONFLICT (source_uri, collection_name)
        DO UPDATE SET last_synced_at = EXCLUDED.last_synced_at
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "source_name": source_name,
            "collection_name": collection_name,
            "last_synced_at": ts
        })

async def update_last_synced(source_name: str, collection_name: str, ts: datetime):
    await run_sync(_update_last_synced_sync, source_name, collection_name, ts)

async def _process_batch(buffer: list[dict[str, Any]], source_name: str, collection: str, loop: asyncio.AbstractEventLoop):
    """
    Helper to process and write a batch in a separate thread.
    Args:
        buffer: List of MongoDB documents
        source_name: The human-readable name of the source (e.g., 'primary')
        collection: The target table name
        loop: The async event loop
    """
    # 1. Convert to DataFrame
    df = pd.DataFrame(buffer)
    
    # 2. Clean and Transform
    df = clean_dataframe(df)
    
    # 3. Add Metadata
    df["_source"] = source_name  # Store the name ('primary') instead of the URI
    df["_synced_at"] = datetime.now(UTC)
    
    # 4. Write to SQL (Blocking I/O offloaded to thread)
    await loop.run_in_executor(
        None,
        partial(write_df_to_sql_sync, df, collection)
    )

def _log_start_history(history_id: str, source: str, collection: str):
    query = text("""
        INSERT INTO sync_history (id, source, collection, status, started_at, message)
        VALUES (:id, :source, :collection, 'STARTED', :now, 'Sync started')
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "id": history_id,
            "source": source,
            "collection": collection,
            "now": datetime.now(UTC)
        })

def _log_end_history(history_id: str, status: str, records: int, message: str):
    query = text("""
        UPDATE sync_history 
        SET status = :status, completed_at = :now, records_synced = :records, message = :message
        WHERE id = :id
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "id": history_id,
            "status": status,
            "now": datetime.now(UTC),
            "records": records,
            "message": message
        })

async def sync_collection_streaming(source_name: str, source_uri: str, collection_name: str):
    """
    Orchestrates the streaming sync.
    Args:
        source_name: 'primary' (used for tracking/logging)
        source_uri: 'mongodb://localhost...' (used for connection)
        collection_name: 'users'
    """
    history_id = str(uuid.uuid4())
    await run_sync(_log_start_history, history_id, source_name, collection_name)

    batch_size = settings.SYNC_BATCH_SIZE
    logger.info(f"Starting sync for {collection_name} from {source_name}")
    
    # Connect using the URI
    client = get_mongo_client(source_uri)
    try:
        # Try to get the database defined in the URI
        default_db = client.get_default_database()
        db = default_db
    except Exception:
        # Fallback to config name
        db = client[settings.MONGO_DB_NAME]

    # Check metadata using the NAME
    last_synced = await get_last_synced(source_name, collection_name)
    query = {}
    if last_synced:
        logger.info(f"[{source_name}] Resuming sync from {last_synced}")
        query["updated_at"] = {"$gt": last_synced}
    else:
        logger.info(f"[{source_name}] Full sync started (no checkpoint found)")

    cursor = db[collection_name].find(query).sort("updated_at", 1)
    
    buffer = []
    count = 0
    latest_timestamp = last_synced
    
    # Get current loop for offloading
    loop = asyncio.get_running_loop()

    try:
        logger.info(f"[{source_name}] Started fetching documents...")
        start_time = datetime.now(UTC)
        async for doc in cursor:
            buffer.append(doc)
            
            # High-water mark tracking
            updated_at = doc.get("updated_at")
            if isinstance(updated_at, datetime):
                if latest_timestamp is None or updated_at > latest_timestamp:
                    latest_timestamp = updated_at

            if len(buffer) >= batch_size:
                # Call helper with exactly 4 arguments
                logger.debug(f"[{source_name}] Processing batch of {len(buffer)} records...")
                await _process_batch(buffer, source_name, collection_name, loop)
                count += len(buffer)
                buffer = []

        # Process remaining buffer
        if buffer:
            await _process_batch(buffer, source_name, collection_name, loop)
            count += len(buffer)

        # Update checkpoint using the NAME
        if latest_timestamp:
            await update_last_synced(source_name, collection_name, latest_timestamp)

        duration = (datetime.now(UTC) - start_time).total_seconds()
        msg = f"Synced {count} rows from {source_name} to {collection_name} in {duration:.2f}s"
        logger.info(msg)
        
        await run_sync(_log_end_history, history_id, "SUCCESS", count, msg)
        return msg

    except Exception as e:
        logger.exception(f"Failed to sync {collection_name} from {source_name}: {e}")
        await run_sync(_log_end_history, history_id, "FAILED", count, str(e))
        raise