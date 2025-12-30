import logging
import sys
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis

from src.api.routes import router as api_router
from src.config import settings
from src.core.db import close_mongo_clients, init_metadata_table, run_sync
from src.core.logger import setup_logging
from src.services.etl import sync_collection_streaming

# Setup Logging
setup_logging()
logger = logging.getLogger("main")

# --- AUTOMATION SCHEDULER ---
scheduler = AsyncIOScheduler()

async def scheduled_sync_job():
    """
    This function runs automatically. 
    It iterates through all sources and syncs the configured collection.
    """
    logger.info("⏳ Scheduled Sync Job Started...")
    
    # Define which collections you want to auto-sync
    # You might want to move this to config.py later
    TARGET_COLLECTIONS = ["ticket", "users"] 

    for collection in TARGET_COLLECTIONS:
        for name, uri in settings.MONGO_SOURCES.items():
            try:
                logger.info(f"Auto-syncing {collection} from {name}...")
                await sync_collection_streaming(name, uri, collection)
            except Exception as e:
                logger.error(f"Auto-sync failed for {name}/{collection}: {e}")
                
    logger.info("Scheduled Sync Job Finished.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up sync service...")
    
    # Verify Redis connection
    try:
        r = Redis.from_url(settings.REDIS_URL)
        await r.ping()
        await r.close()
        logger.info("Redis connection verified.")
    except Exception as e:
        logger.error(f"FATAL: Redis connection failed: {e}")
        sys.exit(1)

    try:
        # init_metadata_table is sync, run it in executor
        await run_sync(init_metadata_table)
        logger.info("Metadata table checked/created.")
        
        # Start Scheduler
        scheduler.start()
        scheduler.add_job(scheduled_sync_job, "interval", minutes=60, id="auto_sync")
        logger.info("Scheduler started with auto_sync job (every 60m).")
        
    except Exception as e:
        logger.error(f"Failed to init DB: {e}")
        raise
    
    yield
    
    logger.info("Shutting down...")
    scheduler.shutdown()
    close_mongo_clients()

app = FastAPI(title="ETL Sync Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In strict prod, restrict this.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)