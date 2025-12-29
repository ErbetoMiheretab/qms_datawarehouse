# QMS Data Warehouse Sync Service

A robust, streaming data synchronization service that replicates MongoDB collections to PostgreSQL. Built with FastAPI, Motor, and SQLAlchemy.

## Features

- **Streaming Sync**: Efficiently streams data from MongoDB to PostgreSQL using low-memory cursors and `COPY` commands.
- **Resumable**: Tracks the `updated_at` timestamp to resume sync from where it left off.
- **Batched Processing**: Configurable batch sizes for optimal performance.
- **Authentication**: Secured via API Key.
- **Dockerized**: Ready for deployment with Docker Compose.

## Prerequisites

- Python 3.12+
- Docker & Docker Compose

## Setup

1. **Clone the repository**
   ```bash
   git clone <repo_url>
   cd qms-datawarehouse
   ```

2. **Environment Configuration**
   Copy `.env.example` to `.env` (or create one) and set the following:
   ```env
   # Comma-separated list of MongoDB Connection Strings
   MONGO_SOURCES=mongodb://localhost:27017/source_db

   # Target PostgreSQL Data Warehouse
   WAREHOUSE_URL=postgresql://user:pass@localhost:5432/analytics

   # Redis for Task Queue
   REDIS_URL=redis://localhost:6379/0

   # Security
   API_KEY=your-secret-api-key
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Running the Service

### With Docker (Recommended)
```bash
docker-compose up --build
```

### Local Development
1. Start infrastructure (Postgres, Redis, MongoDB):
   ```bash
   docker-compose up -d mongo-a warehouse redis
   ```
2. Run the application:
   ```bash
   uvicorn src.main:app --reload
   ```

## Usage

### Health Check
```bash
curl http://localhost:8000/health
```

### Trigger Sync
To sync a collection (e.g., `employees`) from all configured MongoDB sources:

```bash
curl -X POST http://localhost:8000/sync/employees \
  -H "X-API-Key: your-secret-api-key"
```

Response:
```json
{
  "task_id": "uuid-string",
  "status": "started",
  "sources_count": 1
}
```

### Check Status
```bash
curl http://localhost:8000/sync/status/uuid-string \
  -H "X-API-Key: your-secret-api-key"
```

## Testing

Run the test suite using `pytest`. Note that integration tests require Docker to be installed to spin up test containers.

```bash
pytest
```

## Linting

```bash
ruff check .
```
