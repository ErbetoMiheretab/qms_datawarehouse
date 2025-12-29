# src/main.py
import logging
import os
import sys
import uuid

from dotenv import load_dotenv

load_dotenv()

from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field
from redis import Redis

from src.db import close_mongo_clients, init_metadata_table
from src.sync import sync_collection_streaming

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("main")

# Config
MONGO_SOURCES_STR = os.getenv("MONGO_SOURCES", "")
MONGO_SOURCES = [uri.strip() for uri in MONGO_SOURCES_STR.split(",") if uri.strip()]
API_KEY = os.getenv("API_KEY")
API_KEY_NAME = "X-API-Key"

if not MONGO_SOURCES:
    raise RuntimeError("MONGO_SOURCES env var must be comma-separated MongoDB URIs")

if not API_KEY:
    logger.warning("API_KEY not set! Authentication will be disabled (NOT RECOMMENDED for production).")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
try:
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    raise

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    if not API_KEY:
        return None  # No auth configured
    if api_key_header == API_KEY:
        return api_key_header
    raise HTTPException(status_code=403, detail="Could not validate credentials")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up sync service...")
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
    redis_client.hset(task_id, source_uri, "running")
    try:
        result = await sync_collection_streaming(source_uri, collection)
        redis_client.hset(task_id, source_uri, f"success: {result}")
    except Exception as e:
        error_msg = f"failed: {e!s}"
        redis_client.hset(task_id, source_uri, error_msg)
        logger.error(f"Task {task_id} [{source_uri}] failed: {e}")

class SyncRequest(BaseModel):
    collection: str = Field(..., pattern=r"^[a-zA-Z0-9_-]+$")

@app.post("/sync/{collection}", dependencies=[Depends(get_api_key)])
async def trigger_sync(collection: str, background_tasks: BackgroundTasks):
    # Validate collection name via path param (fastapi does this, but we added regex above)
    # Re-using logic or just relying on path param.
    # Let's support regex check here too just in case.
    if not collection.replace("_", "").replace("-", "").isalnum():
         raise HTTPException(400, "Invalid collection name")

    task_id = str(uuid.uuid4())
    
    redis_client.hset(task_id, "meta:total_sources", len(MONGO_SOURCES))
    redis_client.hset(task_id, "meta:collection", collection)
    redis_client.expire(task_id, 3600)  # auto-expire after 1h

    for uri in MONGO_SOURCES:
        background_tasks.add_task(run_sync_task, task_id, uri, collection)

    return {"task_id": task_id, "status": "started", "sources_count": len(MONGO_SOURCES)}

@app.get("/sync/status/{task_id}", dependencies=[Depends(get_api_key)])
async def get_sync_status(task_id: str):
    data = redis_client.hgetall(task_id)
    if not data:
        raise HTTPException(404, "Task not found or expired")
    return {"task_id": task_id, "details": data}
