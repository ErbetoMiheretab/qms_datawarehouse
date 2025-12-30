from fastapi import Security
from fastapi.security.api_key import APIKeyHeader

from src.config import settings

api_key_header = APIKeyHeader(name=settings.API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    # if not settings.API_KEY:
    #     return None  # Auth disabled
    # if api_key_header == settings.API_KEY:
    #     return api_key_header
    # raise HTTPException(status_code=403, detail="Could not validate credentials")

    if not settings.API_KEY:
        return None  