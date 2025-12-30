# Use uv image for fast builds
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set environment variables
# UV_SYSTEM_PYTHON=1 tells uv to install into the system python environment (no venv needed in container)
ENV UV_SYSTEM_PYTHON=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies using uv
COPY requirements.txt .
RUN uv pip install --no-cache -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Use uvicorn for production (or gunicorn with uvicorn workers)
# Using uvicorn directly for simplicity as per requirements
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
