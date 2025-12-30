# src/schemas/request.py
from pydantic import BaseModel, Field

class SyncRequest(BaseModel):
    collection: str = Field(..., pattern=r"^[a-zA-Z0-9_-]+$")