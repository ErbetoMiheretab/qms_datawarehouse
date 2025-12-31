# src/schemas/request.py
from datetime import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class SyncRequest(BaseModel):
    collection: str = Field(..., pattern=r"^[a-zA-Z0-9_-]+$")

class SyncResponse(BaseModel):
    task_id: str
    status: str
    sources_count: int

class HealthComponent(BaseModel):
    redis: str
    database: str

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    components: HealthComponent