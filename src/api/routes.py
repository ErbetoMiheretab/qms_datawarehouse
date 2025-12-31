import logging
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from redis.asyncio import Redis

from src.api.deps import get_api_key, limiter
from src.config import settings
from src.schemas.request import SyncRequest, SyncResponse, HealthResponse
from src.services.etl import sync_collection_streaming

router = APIRouter()
logger = logging.getLogger("api")

# Redis connection
redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)

async def run_sync_task(task_id: str, source_name: str, source_uri: str, collection: str):
    """Background task wrapper."""
    await redis_client.hset(task_id, source_uri, "running")
    logger.info(f"Task {task_id} started for {collection} from source {source_name}")
    try:
        result = await sync_collection_streaming(source_name, source_uri, collection)
        await redis_client.hset(task_id, source_uri, f"success: {result}")
        logger.info(f"Task {task_id} completed for source {source_name}: {result}")
    except Exception as e:
        await redis_client.hset(task_id, source_uri, f"failed: {e!s}")
        logger.error(f"Task {task_id} failed for source {source_name}: {e}")

@router.get("/health", response_model=HealthResponse)
@limiter.limit("10/minute")
async def health(request: Request):
    """
    Health check endpoint that verifies connectivity to:
    - Redis
    - PostgreSQL (via Main DB)
    """
    status = {
        "status": "healthy",
        "timestamp": datetime.now(UTC).isoformat(),
        "components": {
            "redis": "unknown",
            "database": "unknown"
        }
    }
    
    # Check Redis
    try:
        if await redis_client.ping():
            status["components"]["redis"] = "healthy"
    except Exception as e:
        status["components"]["redis"] = f"unhealthy: {e}"
        status["status"] = "degraded"

    # Check DB
    try:
        # Simple query to check DB connection
        from sqlalchemy import text
        from src.core.db import run_sync, engine
        
        def check_db():
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        
        await run_sync(check_db)
        status["components"]["database"] = "healthy"
    except Exception as e:
        status["components"]["database"] = f"unhealthy: {e}"
        status["status"] = "degraded"

    if status["status"] != "healthy":
        raise HTTPException(status_code=503, detail=status)
        
    return status

@router.post("/sync/{collection}", dependencies=[Depends(get_api_key)], response_model=SyncResponse)
@limiter.limit("5/minute")
async def trigger_sync(request: Request, collection: str, background_tasks: BackgroundTasks):
    logger.info(f"Received sync request for collection: {collection}")
    
    # Validate Collection Name using Pydantic
    try:
        SyncRequest(collection=collection)
    except Exception:
         logger.warning(f"Invalid collection name rejected: {collection}")
         raise HTTPException(400, "Invalid collection name format")

    task_id = str(uuid.uuid4())
    
    await redis_client.hset(task_id, "meta:total_sources", len(settings.MONGO_SOURCES))
    await redis_client.hset(task_id, "meta:collection", collection)
    await redis_client.expire(task_id, 3600)

    for name, uri in settings.MONGO_SOURCES.items():
        background_tasks.add_task(run_sync_task, task_id, name, uri, collection)

    return {
        "task_id": task_id, 
        "status": "started", 
        "sources_count": len(settings.MONGO_SOURCES.keys())
    }

@router.get("/sync/status/{task_id}", dependencies=[Depends(get_api_key)])
@limiter.limit("60/minute")
async def get_sync_status(request: Request, task_id: str):
    logger.debug(f"Checking status for task {task_id}")
    data = await redis_client.hgetall(task_id)
    if not data:
        logger.warning(f"Status check failed: Task {task_id} not found")
        raise HTTPException(404, "Task not found or expired")
    return {"task_id": task_id, "details": data}

def _fetch_sync_logs(limit: int):
    from sqlalchemy import text
    from src.core.db import engine
    
    query = text("""
        SELECT * FROM sync_history 
        ORDER BY started_at DESC 
        LIMIT :limit
    """)
    with engine.connect() as conn:
        # Use mappings() to get dictionary-like rows
        result = conn.execute(query, {"limit": limit}).mappings().all()
        return [dict(row) for row in result]

@router.get("/sync/logs", dependencies=[Depends(get_api_key)])
async def get_sync_logs(limit: int = 50):
    from src.core.db import run_sync
    try:
        logs = await run_sync(_fetch_sync_logs, limit)
        return {"logs": logs}
    except Exception as e:
        logger.error(f"Failed to fetch logs: {e}")
        raise HTTPException(500, "Internal Server Error")