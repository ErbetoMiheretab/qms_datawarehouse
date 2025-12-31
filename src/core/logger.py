import logging
import sys
from src.config import settings

def setup_logging():
    """
    Configures the root logger with a standard format and level based on settings.
    """
    log_level = logging.DEBUG if settings.DEBUG else logging.INFO
    
    # Determine format based on environment/config
    if settings.DEBUG:
        # Human-readable for dev
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    else:
        # JSON for production (Splunk, ELK, etc.)
        from pythonjsonlogger import jsonlogger
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s %(filename)s %(lineno)d"
        )
    
    # Configure Root Logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicates during reloads
    if root_logger.handlers:
        root_logger.handlers.clear()
        
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    
    # Set levels for noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.INFO)

    logging.info(f"Logging configured at level: {logging.getLevelName(log_level)}", extra={"event": "startup"})
