
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health(async_client: AsyncClient):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
async def test_auth_missing(async_client: AsyncClient):
    # Should fail without API key
    response = await async_client.post("/sync/test_collection")
    assert response.status_code == 403

@pytest.mark.asyncio
async def test_auth_success(async_client: AsyncClient):
    # Should accept with API key
    # Note: background tasks run immediately in memory with TestClient but with AsyncClient we verify response only
    headers = {"X-API-Key": "test-secret"}
    response = await async_client.post("/sync/test_collection", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    assert data["status"] == "started"

@pytest.mark.asyncio
async def test_invalid_collection_name(async_client: AsyncClient):
    headers = {"X-API-Key": "test-secret"}
    response = await async_client.post("/sync/bad;name", headers=headers)
    assert response.status_code == 400
