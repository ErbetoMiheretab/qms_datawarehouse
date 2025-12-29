# src/main.py
import os
import logging
import sys
import uuid
from dotenv import load_dotenv
load_dotenv()

from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, HTTPException
from redis import Redis
from src.sync import sync_collection_streaming
from src.db import init_metadata_table, close_mongo_clients

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("main")

# Config: Expecting comma-separated string, e.g., "mongo://a,mongo://b"
MONGO_SOURCES_STR = os.getenv("MONGO_SOURCES", "")
MONGO_SOURCES = [uri.strip() for uri in MONGO_SOURCES_STR.split(",") if uri.strip()]

if not MONGO_SOURCES:
    raise RuntimeError("MONGO_SOURCES env var must be comma-separated MongoDB URIs")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis_client = Redis.from_url(REDIS_URL, decode_responses=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up sync service...")
    # Initialize Postgres Table
    try:
        init_metadata_table()
        logger.info("Metadata table checked/created.")
    except Exception as e:
        logger.error(f"Failed to init DB: {e}")
        raise
    
    yield
    
    logger.info("Shutting down...")
    close_mongo_clients()
    redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok"}

async def run_sync_task(task_id: str, source_uri: str, collection: str):
    # Use the source_uri as the key field to avoid overwriting other sources
    redis_client.hset(task_id, source_uri, "running")
    try:
        result = await sync_collection_streaming(source_uri, collection)
        redis_client.hset(task_id, source_uri, f"success: {result}")
    except Exception as e:
        error_msg = f"failed: {str(e)}"
        redis_client.hset(task_id, source_uri, error_msg)
        logger.error(f"Task {task_id} [{source_uri}] failed: {e}")

@app.post("/sync/{collection}")
async def trigger_sync(collection: str, background_tasks: BackgroundTasks):
    # Basic sanitization
    if not collection.replace("_", "").replace("-", "").isalnum():
        raise HTTPException(400, "Invalid collection name")
    
    task_id = str(uuid.uuid4())
    
    # Initialize task status in Redis
    redis_client.hset(task_id, "meta:total_sources", len(MONGO_SOURCES))
    redis_client.hset(task_id, "meta:collection", collection)
    redis_client.expire(task_id, 3600)  # auto-expire after 1h

    for uri in MONGO_SOURCES:
        # Add tasks to FastAPI background handler
        background_tasks.add_task(run_sync_task, task_id, uri, collection)

    return {"task_id": task_id, "status": "started", "sources_count": len(MONGO_SOURCES)}

@app.get("/sync/status/{task_id}")
async def get_sync_status(task_id: str):
    data = redis_client.hgetall(task_id)
    if not data:
        raise HTTPException(404, "Task not found or expired")
    return {"task_id": task_id, "details": data}