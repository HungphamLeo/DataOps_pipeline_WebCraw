
# PLAN: DataOps_pipeline_WebCraw → Production-Grade (fintech-stock-platform)

**Goal:** Nâng cấp repo WebCraw từ proof-of-concept lên production-ready stock data platform — tập trung vào reliability, observability, data quality, và security. Repo trở thành REPO-01 trong portfolio.

**Scope:** Không refactor toàn bộ codebase. Áp dụng production standards theo từng layer, ưu tiên những gì tuyển dụng Data Engineer coi trọng nhất.

**Non-Goals:** Không thêm ML, không chuyển sang microservices trong plan này. Focus production standards.

## Sub-Task 01 — Exponential Backoff + Jitter cho Web Crawler

**Intent:**
Crawler hiện tại gọi thẳng HTTP requests không có retry logic → khi web target bị rate-limit hoặc timeout, pipeline fail ngay. Production pattern: Exponential Backoff + Full Jitter để tránh Thundering Herd (toàn bộ workers retry cùng lúc).

**Expected Outcomes:**
- Mọi HTTP call trong `platforms/ingestion/base_crawler.py` có retry với exponential backoff (base=1s, cap=60s, jitter=random[0..cap])
- Khi 429 (rate-limit) nhận được, crawler backoff dài hơn (parse `Retry-After` header nếu có)
- Retry attempts log ra với `correlation_id`, `attempt_number`, `wait_seconds` dưới dạng JSON
- Unit test kiểm tra retry không vượt max_attempts

**Todo List:**
1. Đọc `platforms/ingestion/base_crawler.py` — xác định method HTTP call
2. Implement `RetryConfig` dataclass (max_attempts=5, base_delay=1.0, cap=60.0, jitter=True)
3. Wrap HTTP call với `tenacity` hoặc custom retry loop (backoff + jitter)
4. Xử lý HTTP 429: đọc `Retry-After` header, override delay nếu lớn hơn calculated
5. Inject `correlation_id` (UUID) vào từng crawl session; log mỗi attempt với structured JSON
6. Viết `tests/unit/test_retry_logic.py` — mock HTTP 429 → 429 → 200, verify 3 attempts

**Relevant Context:**
- File: `DataOps_pipeline_WebCraw/platforms/ingestion/base_crawler.py`
- Pattern: Factory DI đã có sẵn ở `platforms/` — thêm `RetryConfig` inject qua constructor
- Dependency: `tenacity>=8.2` hoặc `aiohttp` retry middleware (kiểm tra `requirements.txt`)
- Theory: Thundering Herd → Full Jitter randomizes retry windows → servers không bị surge

**Status:** `[ ] pending`

## Sub-Task 02 — PostgreSQL Connection Pool (PgBouncer / SQLAlchemy pool)

**Intent:**
Mỗi Prefect task mở connection PostgreSQL riêng → với 10+ concurrent tasks = 10+ connections × 10MB RAM/connection = 100MB+ wasted. Production standard: connection pool giới hạn max connections, tái sử dụng idle connections.

**Expected Outcomes:**
- `platforms/storage/postgre/base_postgre.py` dùng SQLAlchemy `pool_size=5, max_overflow=10, pool_timeout=30`
- Không có `NullPool` (serverless pattern) trừ Lambda/Prefect serverless deployment
- Connection pool metrics exposed: `pool_size`, `checkedout`, `overflow` → log mỗi 60s
- Bonus: `docker-compose.yml` thêm PgBouncer service (transaction mode) nếu > 50 concurrent workers

**Todo List:**
1. Đọc `platforms/storage/postgre/base_postgre.py` — xem cách tạo engine hiện tại
2. Thêm `pool_size`, `max_overflow`, `pool_pre_ping=True` vào `create_engine()`
3. Implement `PoolMetricsLogger`: background thread log pool stats mỗi 60s dưới dạng JSON
4. Thêm `pool_pre_ping=True` để tự detect stale connections
5. Cập nhật `docker-compose.yml`: thêm `pgbouncer` service (pgbouncer/pgbouncer image) với `pool_mode=transaction`
6. Cập nhật `stack.yaml`: `db.pool_size`, `db.max_overflow`, `db.pgbouncer_enabled` flags

**Relevant Context:**
- File: `DataOps_pipeline_WebCraw/platforms/storage/postgre/base_postgre.py`
- File: `DataOps_pipeline_WebCraw/docker-compose.yml` (nếu có)
- Theory: 10MB/connection × N workers = OOM risk → pool capping là bắt buộc
- Limit: `max_overflow` = (CPU cores × 2) + effective_spindle_count (PostgreSQL wiki formula)

**Status:** `[ ] pending`

## Sub-Task 03 — Delta Lake Shallow Clone → Deep Clone Migration

**Intent:**
Delta Lake Shallow Clone tạo metadata reference đến files của source table. Nếu source `VACUUM` chạy trước khi clone được promote → orphan pointers → data corruption. Production: dùng Deep Clone cho long-lived copies, Shallow Clone chỉ cho short-lived dev testing.

**Expected Outcomes:**
- Tất cả `DeltaTable.clone()` calls trong codebase dùng `shallow=False` (deep clone) cho Bronze→Silver promotion
- Shallow clone chỉ còn trong dev/test flows, có comment rõ ràng lý do
- `VACUUM` retention period trong config: minimum 7 ngày (`delta.deletedFileRetentionDuration = "interval 7 days"`)
- ADR document ghi lại quyết định này

**Todo List:**
1. `grep -r "clone" DataOps_pipeline_WebCraw/` — tìm tất cả clone calls
2. Đọc từng file có clone call — xác định shallow vs deep
3. Chuyển Bronze→Silver promotion clone sang deep clone
4. Set `spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")`
5. Thêm `delta.logRetentionDuration = "interval 30 days"` và `delta.deletedFileRetentionDuration = "interval 7 days"` vào `spark_session.py` config
6. Tạo `docs/adr/ADR-001-delta-clone-strategy.md` — ghi lý do chọn deep clone

---
```go

```
---

**Relevant Context:**
- File: `DataOps_pipeline_WebCraw/platforms/processing/polars/polars_engine.py`
- File: `DataOps_pipeline_WebCraw/platforms/processing/spark_stack/` (nếu có)
- Theory: Shallow Clone → Orphan Pointer risk sau VACUUM → Data Loss Production Incident

**Status:** `[ ] pending`

## Sub-Task 04 — Data Contracts (Great Expectations → Pandera Schema)

**Intent:**
Great Expectations hiện tại chạy sau ingestion nhưng không enforce schema tại ingest point. Production pattern: Data Contract = schema validation tại boundary (crawl → bronze), không để bad data propagate downstream. Nếu schema violation → pipeline halt + alert, không âm thầm ghi sai.

**Expected Outcomes:**
- Mỗi data source có Pandera `DataFrameSchema` (hoặc Pydantic model) định nghĩa expected columns, dtypes, nullable, range constraints
- Schema validation chạy ngay sau `_load_raw()`, trước write vào Bronze
- Violation → raise `SchemaError` → Prefect task failure → alert
- Great Expectations vẫn giữ cho Silver/Gold layer cross-table checks
- Schema artifacts lưu trong `governance/contracts/` dưới dạng YAML

**Todo List:**
1. Đọc `platforms/governance/data_quality/dq_registry.py` — hiểu GE integration hiện tại
2. Cài `pandera>=0.18` (thêm vào `requirements.txt`)
3. Tạo `governance/contracts/stock_price_contract.yaml` — định nghĩa schema: columns, types, constraints
4. Implement `ContractValidator` class: load YAML → `pandera.DataFrameSchema` → validate
5. Inject `ContractValidator` vào `base_crawler.py` pipeline — validate trước Bronze write
6. Prefect task khi `SchemaError`: log structured error với field-level details, mark task FAILED
7. Viết `tests/unit/test_contracts.py` — test schema violation detection

**Relevant Context:**
- File: `DataOps_pipeline_WebCraw/platforms/governance/data_quality/dq_registry.py`
- File: `DataOps_pipeline_WebCraw/sql/migrations/V3__create_normalized_tables.sql` — source of truth columns
- Theory: BFT-inspired Circuit Breaker → Data Contract là fuse ngăn corruption lan rộng

**Status:** `[ ] pending`

## Sub-Task 05 — OpenTelemetry Traces + Structured Logging

**Intent:**
Hiện tại có structlog nhưng không có distributed tracing. Khi pipeline fail, không thể trace từ CLI → Prefect task → Polars job → MinIO write xem bước nào chậm. Production: OpenTelemetry traces với span per pipeline stage, log có `trace_id`, `span_id`, `correlation_id`.

**Expected Outcomes:**
- `opentelemetry-sdk`, `opentelemetry-exporter-otlp` thêm vào `requirements.txt`
- Mỗi Prefect flow khởi tạo OTel tracer; mỗi major step là 1 span (ingest, validate, transform, write)
- Mọi log message có fields: `trace_id`, `span_id`, `correlation_id`, `pipeline_name`, `run_id`, `level`, `timestamp` (ISO 8601), `message`
- `docker-compose.yml` thêm Jaeger (all-in-one) service cho local tracing
- Prometheus `/metrics` endpoint expose: `pipeline_duration_seconds`, `records_processed_total`, `pipeline_errors_total`

**Todo List:**
1. Thêm `opentelemetry-sdk>=1.25`, `opentelemetry-exporter-otlp-proto-grpc>=1.25`, `prometheus-client>=0.20` vào `requirements.txt`
2. Tạo `shared/telemetry.py` — setup OTel tracer, Prometheus metrics registry
3. Wrap Prefect `@flow` entry point với root span: `with tracer.start_as_current_span("pipeline.run")`
4. Thêm child spans tại: `ingest`, `validate_contract`, `transform`, `write_bronze`, `write_silver`
5. Reconfigure structlog `shared/logging.py`: thêm OTel context processor inject `trace_id`, `span_id`
6. `docker-compose.yml`: thêm Jaeger all-in-one + OTEL Collector → Jaeger exporter
7. `docker-compose.yml`: thêm Prometheus + Grafana (dùng image grafana/grafana)

**Relevant Context:**
- File: `DataOps_pipeline_WebCraw/shared/` (logging utilities)
- File: `DataOps_pipeline_WebCraw/platforms/orchestration/prefect/prefect_main.py`
- Standard: OTLP gRPC → localhost:4317 (OTEL Collector) → Jaeger

**Status:** `[ ] pending`

## Sub-Task 06 — GitHub Actions CI/CD Pipeline

**Intent:**
Không có automated testing hay deployment. Mỗi push cần: lint → type check → unit test → data quality check → Docker build. Production: fail fast, block merge nếu test fail, auto-deploy khi merge main.

**Expected Outcomes:**
- `.github/workflows/ci.yml`: trigger on PR và push to main
- Jobs: `lint` (ruff), `typecheck` (mypy), `test` (pytest --cov ≥ 80%), `docker-build`
- `.github/workflows/cd.yml`: trigger on merge to main → push Docker image to registry
- `CHANGELOG.md` tạo mới — template Keep a Changelog
- Pre-commit hooks: `ruff`, `mypy`, `detect-secrets`

**Todo List:**
1. Tạo `.github/workflows/ci.yml` — 4 jobs: lint/typecheck/test/docker
2. Thêm `ruff>=0.6`, `mypy>=1.11`, `pytest-cov>=5.0` vào `requirements.txt` (dev dependencies)
3. Tạo `pyproject.toml` hoặc `setup.cfg` với ruff + mypy config
4. Tạo `.github/workflows/cd.yml` — Docker build + push (GitHub Container Registry)
5. Tạo `CHANGELOG.md` — format Keep a Changelog v1.0
6. Tạo `.pre-commit-config.yaml` với ruff, mypy, detect-secrets hooks
7. Tạo `docs/adr/ADR-002-cicd-strategy.md`

**Relevant Context:**
- File: `DataOps_pipeline_WebCraw/.github/` (kiểm tra existing workflows)
- File: `DataOps_pipeline_WebCraw/requirements.txt`
- Standard: GitHub Actions `ubuntu-latest`, Python 3.11, Docker buildx

**Status:** `[ ] pending`

## Sub-Task 07 — TimescaleDB / PostgreSQL Advanced Schema

**Intent:**
`V3__create_normalized_tables.sql` có fact_stock_price nhưng không có time-series optimization. Với dữ liệu tick-by-tick, query "price in last 7 days" trên plain PostgreSQL table sẽ chậm khi data > 10M rows. Production: partition by time, index trên `(symbol, timestamp)`, hoặc migrate sang TimescaleDB hypertable.

**Expected Outcomes:**
- `fact_stock_price` có `PARTITION BY RANGE (trade_date)` hoặc TimescaleDB hypertable
- Index: `CREATE INDEX ON fact_stock_price (symbol_id, trade_date DESC)` (covering index cho dashboard queries)
- `dim_company` có SCD Type 2 full implementation: `valid_from`, `valid_to`, `is_current` columns với MERGE logic
- Migration file `V4__add_partitioning_and_scd2.sql` tạo mới

**Todo List:**
1. Đọc `sql/migrations/V3__create_normalized_tables.sql` — xem schema hiện tại
2. Quyết định: TimescaleDB vs PostgreSQL native partitioning (ghi trong ADR-003)
3. Tạo `sql/migrations/V4__add_time_partitioning.sql` — partition `fact_stock_price` by `trade_date` monthly
4. Thêm covering indexes: `(symbol_id, trade_date DESC) INCLUDE (close_price, volume)`
5. Implement SCD2 MERGE procedure cho `dim_company`: detect change → expire old row → insert new
6. Tạo `sql/migrations/V5__scd2_dim_company.sql`
7. Cập nhật `governance/analyze_business_context.md` với schema rationale

**Relevant Context:**
- File: `DataOps_pipeline_WebCraw/sql/migrations/V3__create_normalized_tables.sql`
- File: `DataOps_pipeline_WebCraw/governance/analyze_business_context.md`
- Theory: B-tree index on (symbol, date DESC) → Index Only Scan cho time-range queries

**Status:** `[ ] pending`

## Dependency Order

---
```
ST01 (Retry/Backoff)        → ST05 (OTel Logging — cần correlation_id từ ST01)
ST04 (Data Contracts)       → ST05 (Traces bao gồm contract validation span)
ST02 (Connection Pool)      → ST07 (Schema — cần pool trước khi test schema perf)
ST03 (Clone Strategy)       → ST07 (VACUUM config liên quan)
ST06 (CI/CD)                → tất cả (CI test tất cả changes)
```
---

## Production Checklist

| Concern | Sub-Task | Theory Applied |
|---------|----------|----------------|
| Rate-limit resilience | ST01 | Exponential Backoff + Full Jitter → Thundering Herd |
| DB connection exhaustion | ST02 | PgBouncer → 10MB/connection → OOM prevention |
| Data corruption | ST03 | Shallow Clone + VACUUM → Orphan Pointer |
| Schema drift | ST04 | BFT-style Contract as Circuit Breaker |
| Observability | ST05 | OpenTelemetry → Root Cause Analysis |
| Automation | ST06 | DORA metrics → Deployment Frequency |
| Query performance | ST07 | B-tree Covering Index → Index Only Scan |
