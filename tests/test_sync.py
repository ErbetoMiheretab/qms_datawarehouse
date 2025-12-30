from datetime import datetime

import pytest
import src.db
from sqlalchemy import text
from src.db import get_mongo_client
from src.sync import sync_collection_streaming


@pytest.mark.asyncio
async def test_sync_logic(mongo_container, postgres_container):
    # 1. seed mongo
    mongo_uri = mongo_container.get_connection_url().replace("localhost", "127.0.0.1")
    client = get_mongo_client(mongo_uri)
    db = client["source_db"]
    collection = db["employees"]
    
    # cleanup
    await collection.delete_many({})
    
    now = datetime.now()
    docs = [
        {"_id": "1", "name": "Alice", "role": "engineer", "meta": None, "updated_at": now},
        {"_id": "2", "name": "Bob", "role": "manager", "meta": None, "updated_at": now},
        {"_id": "3", "name": "Charlie", "role": None, "meta": {"foo": "bar"}, "updated_at": now}
    ]
    await collection.insert_many(docs)
    
    # 2. run sync
    result = await sync_collection_streaming(mongo_uri, "employees", batch_size=2)
    assert "Synced 3 rows" in result
    
    # 3. verify postgres
    with src.db.engine.connect() as conn:
        # Check metadata
        meta = conn.execute(text("SELECT * FROM sync_metadata WHERE source_uri = :u"), {"u": mongo_uri}).fetchone()
        assert meta is not None
        
        # Check data table
        # Table name is 'employees'
        rows = conn.execute(text("SELECT * FROM employees")).fetchall()
        assert len(rows) == 3
        
        # Verify JSON handling
        names = sorted([r.name for r in rows])
        assert names == ["Alice", "Bob", "Charlie"]
