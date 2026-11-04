# ==============================================================================
# Project SENTINEL — Polyglot FastAPI RAG Investigator Assistant
# Production Container Specification (Python 3.11 Slim)
# ==============================================================================

FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8000

# Install minimal OS runtime dependencies (curl for container healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first for caching efficiency
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code and artifacts
COPY app/ ./app/
COPY scripts/ ./scripts/
COPY architecture/ ./architecture/
COPY outputs/ ./outputs/
COPY run_investigator_rag_pipeline.py .

# Expose FastAPI service port
EXPOSE 8000

# Healthcheck probe against FastAPI /health endpoint
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command to run FastAPI under Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]

