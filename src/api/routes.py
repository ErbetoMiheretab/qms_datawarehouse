import logging
import uuid

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from redis import Redis

from src.api.deps import get_api_key
from src.config import settings
from src.services.etl import sync_collection_streaming

# from src.schemas.request import SyncRequest
router = APIRouter()
logger = logging.getLogger("api")

# Redis connection (could also be moved to core/database.py if complex)
redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)

async def run_sync_task(task_id: str,source_name: str ,source_uri: str, collection: str):
    """Background task wrapper."""
    redis_client.hset(task_id, source_uri, "running")
    try:
        result = await sync_collection_streaming(source_name, source_uri, collection)
        redis_client.hset(task_id, source_uri, f"success: {result}")
    except Exception as e:
        redis_client.hset(task_id, source_uri, f"failed: {e!s}")
        logger.error(f"Task {task_id} failed: {e}")

@router.get("/health")
async def health():
    return {"status": "ok"}

@router.post("/sync/{collection}", dependencies=[Depends(get_api_key)])
async def trigger_sync(collection: str, background_tasks: BackgroundTasks):
    # Double check regex manual validation if needed, though Pydantic handles validation
    if not collection.replace("_", "").replace("-", "").isalnum():
         raise HTTPException(400, "Invalid collection name")

    task_id = str(uuid.uuid4())
    
    redis_client.hset(task_id, "meta:total_sources", len(settings.MONGO_SOURCES))
    redis_client.hset(task_id, "meta:collection", collection)
    redis_client.expire(task_id, 3600)

    for name,uri in settings.MONGO_SOURCES.items():
        background_tasks.add_task(run_sync_task, task_id,name, uri, collection)

    return {
        "task_id": task_id, 
        "status": "started", 
        "sources_count": len(settings.MONGO_SOURCES.keys())
    }

@router.get("/sync/status/{task_id}", dependencies=[Depends(get_api_key)])
async def get_sync_status(task_id: str):
    data = redis_client.hgetall(task_id)
    if not data:
        raise HTTPException(404, "Task not found or expired")
    return {"task_id": task_id, "details": data}