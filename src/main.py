# src/main.py
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis import Redis

from src.api.routes import router as api_router
from src.config import settings
from src.core.db import close_mongo_clients, init_metadata_table

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("main")

# Verify Redis connection on startup
try:
    r = Redis.from_url(settings.REDIS_URL)
    r.ping()
    r.close()
except Exception as e:
    logger.error(f"FATAL: Redis connection failed: {e}")
    sys.exit(1)

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

app = FastAPI(title="ETL Sync Service", lifespan=lifespan)
app.include_router(api_router)