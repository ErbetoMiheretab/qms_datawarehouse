import os
from sqlalchemy import create_engine, text

# Default credentials from docker-compose.yml
DEFAULT_URL = "postgresql://admin:supersecretpassword@localhost:5431/analytics"

def test_connection():
    print(f"Testing connection to: {DEFAULT_URL}")
    try:
        engine = create_engine(DEFAULT_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ Connection successful!")
            print(f"Result: {result.fetchone()}")
    except Exception as e:
        print("❌ Connection failed!")
        print(f"Error: {e}")

if __name__ == "__main__":
    test_connection()
