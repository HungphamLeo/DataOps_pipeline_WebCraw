# Pin bookworm cứng để base image ổn định, tránh bị re-tag sang Debian mới.
FROM python:3.11.13-slim-bookworm

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

# Copy source — không COPY config.py (file không tồn tại ở root)
COPY cli ./cli
COPY flows ./flows
COPY platforms ./platforms
COPY shared ./shared
COPY scripts ./scripts
# logs/ được mount qua volume khi runtime; tạo thư mục sẵn với quyền write
RUN mkdir -p /app/logs

# Non-root user — giảm attack surface
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser \
    && chown -R appuser:appgroup /app
USER appuser

CMD ["python", "-m", "cli.main"]
