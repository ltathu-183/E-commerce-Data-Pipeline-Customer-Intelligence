# Use official Python slim image
FROM python:3.11-slim

# Install uv (Astral's fast Python package manager)
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy dependency files first (leverages Docker cache)
COPY pyproject.toml uv.lock* ./

# Install dependencies using uv
# --system: install to system Python (no venv inside container)
# --no-cache: avoid caching wheels to keep image lean
RUN uv pip install --system --no-cache -r pyproject.toml || uv pip install --system --no-cache .

# Copy application code
COPY src/ ./src/

# Create optional folders to avoid COPY errors
RUN mkdir -p scripts data/raw data/processed

# Environment defaults
ENV PYTHONUNBUFFERED=1 \
    USE_DATABASE=true \
    ENABLE_NLP=false \
    DATA_RAW=/app/data/raw \
    DATA_PROCESSED=/app/data/processed

# Default command
CMD ["python", "src/etl_pipeline.py"]