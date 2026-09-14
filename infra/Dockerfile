FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    curl \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps from actual repo file
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Copy source folders that really exist in this repo
COPY cli ./cli
COPY flows ./flows
COPY platforms ./platforms
COPY shared ./shared
COPY scripts ./scripts
COPY sql ./sql
COPY logs ./logs

# Optional: copy env file if project expects it
COPY .env .env

# Default command: show CLI help
CMD ["python", "-m", "cli.main", "--help"]