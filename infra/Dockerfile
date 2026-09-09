FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc build-essential curl libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---- requirements (cache friendly)
COPY requirements_common.txt .
COPY requirements_prefect.txt .

RUN pip install --no-cache-dir -r requirements_common.txt \
    && pip install --no-cache-dir -r requirements_prefect.txt

# ---- source code
COPY shared ./shared
COPY platforms ./platforms
COPY scripts ./scripts
COPY services ./services
COPY tests ./tests
COPY tmp ./tmp

# ---- entrypoint
COPY infra/docker/processing/prefect/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PREFECT_API_URL=http://prefect-server:4200/api
ENV PYTHONPATH=/app:/app/shared:/app/platforms:/app/scripts

ENTRYPOINT ["/entrypoint.sh"]


FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc build-essential curl libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---- requirements
COPY requirements_common.txt .
COPY requirements_dbt_postgre.txt .

RUN pip install --no-cache-dir -r requirements_common.txt \
    && pip install --no-cache-dir -r requirements_dbt_postgre.txt

# ---- dbt project
COPY shared ./shared
COPY platforms ./platforms
COPY scripts ./scripts
COPY services ./services
COPY tests ./tests
COPY tmp ./tmp


# ---- entrypoint

ENV PYTHONPATH=/app:/app/shared
ENV DBT_PROJECT_DIR=/app/dbt
ENV DBT_PROFILES_DIR=/app/dbt/profiles

ENTRYPOINT ["dbt"]
