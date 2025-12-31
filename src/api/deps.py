from fastapi import HTTPException, Security, Request
from fastapi.security.api_key import APIKeyHeader
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from src.config import settings
import logging

# Initialize Rate Limiter
limiter = Limiter(key_func=get_remote_address)

api_key_header = APIKeyHeader(name=settings.API_KEY_NAME, auto_error=False)
logger = logging.getLogger("auth")

async def get_api_key(api_key_header: str = Security(api_key_header)):
    # --- TEMPORARY AUTH DISABLE ---
        return api_key_header
    # ------------------------------

    # if not settings.API_KEY:
    #     # If API KEY is not set in env, we allow access but warn (or you could block)
    #     # For security, we should probably fail safe, but sticking to existing logic pattern:
    #     if settings.DEBUG:
    #         logger.warning("Auth skipped: API_KEY not set (DEBUG mode)")
    #         return None
    #     logger.critical("Auth failed: API_KEY not configured on server")
    #     raise HTTPException(status_code=500, detail="Server Misconfiguration: No API Key Set")

    # if api_key_header == settings.API_KEY:
    #     logger.debug("Auth successful: Valid API key provided")
    #     return None
    
    # logger.warning(f"Auth failed: Invalid API key provided. received: ...{api_key_header[-4:] if api_key_header else 'None'}")
    # raise HTTPException(status_code=403, detail="Could not validate credentials")
  