import os

import pytest
from httpx import AsyncClient
from redis import Redis
from sqlalchemy import create_engine
from testcontainers.mongodb import MongoDbContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

# Set environment variables before importing app
# These placeholders are needed before we verify them
os.environ["API_KEY"] = "test-secret"
os.environ["MONGO_SOURCES"] = "mongodb://mock"
os.environ["WAREHOUSE_URL"] = "postgresql://mock"
os.environ["REDIS_URL"] = "redis://mock"

# Import app modules AFTER setting env vars
import src.db
import src.main
from src.db import close_mongo_clients, init_metadata_table
from src.main import app

# -- Global Containers --

@pytest.fixture(scope="session")
def mongo_container():
    with MongoDbContainer("mongo:latest") as mongo:
        yield mongo

@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:latest") as postgres:
        yield postgres

@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:alpine") as redis:
        yield redis

# -- Fixtures --

@pytest.fixture(autouse=True)
def patch_globals(mongo_container, postgres_container, redis_container, monkeypatch):
    """
    Patches global variables in the src modules to point to test containers.
    This fixture automatically runs for every test.
    """
    # 1. Get Container Configs
    mongo_uri = mongo_container.get_connection_url().replace("localhost", "127.0.0.1")
    postgres_uri = postgres_container.get_connection_url().replace("localhost", "127.0.0.1")
    
    # TestContainers Redis helper sometimes gives local host, ensuring we get a valid URI
    redis_port = redis_container.get_exposed_port(6379)
    redis_host = redis_container.get_container_host_ip()
    if redis_host == "localhost":
        redis_host = "127.0.0.1"
    redis_uri = f"redis://{redis_host}:{redis_port}/0"
    
    # 2. Patch Environment Variables
    monkeypatch.setenv("MONGO_SOURCES", mongo_uri)
    monkeypatch.setenv("WAREHOUSE_URL", postgres_uri)
    monkeypatch.setenv("REDIS_URL", redis_uri)
    monkeypatch.setenv("API_KEY", "test-secret")

    # 3. Patch Redis Client in main
    # Close existing (mock) if any
    if hasattr(src.main, "redis_client"):
        try:
            src.main.redis_client.close()
        except Exception:
            pass
    src.main.redis_client = Redis.from_url(redis_uri, decode_responses=True)
    src.main.MONGO_SOURCES = [mongo_uri]

    # 4. Patch SQLAlchemy Engine in db
    # We create a new engine for the test DB
    test_engine = create_engine(postgres_uri)
    # Dispose old engine if we can, or just overwrite the reference
    original_engine = src.db.engine
    src.db.engine = test_engine
    
    # Initialize DB schema
    try:
        init_metadata_table()
    except Exception as e:
        print(f"Failed to init test DB: {e}")
        raise

    yield
    
    # Teardown
    src.main.redis_client.close()
    test_engine.dispose()
    close_mongo_clients()
    
    # Restore original (optional, but good practice if tests were not forked)
    src.db.engine = original_engine


from httpx import AsyncClient, ASGITransport

@pytest.fixture
async def async_client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
