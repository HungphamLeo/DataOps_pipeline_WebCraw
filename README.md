# DataOps Pipeline WebCraw

ELT data platform thu thập dữ liệu tài chính từ [Cophieu68](https://cophieu68.vn), lưu dữ liệu raw trên MinIO và phục vụ dữ liệu đã chuẩn hóa qua PostgreSQL.

Repository này sử dụng **Polars/Python trực tiếp cho transformation**, không dùng dbt. dbt được dành cho repository khác.

---

## 1. Tổng quan hoạt động ELT

```text
Cophieu68 HTTP/HTML
        │
        ▼
  Ingestion / Crawler
        │
        ▼
  Bronze — raw Parquet trên MinIO
        │
        ▼
  Silver — Polars typed, cleaned, deduplicated Parquet
        │
        ▼
  PostgreSQL staging — raw-compatible TEXT
        │
        ▼
  PostgreSQL serving/Gold — normalized typed tables
```

Pipeline được điều phối bởi `Cophieu68PipelineOrchestrator` và có ba phase chính:

1. **Bronze:** gọi nguồn Cophieu68, kiểm tra dữ liệu cơ bản và ghi raw records.
2. **Silver:** đọc Bronze bằng Polars, chuẩn hóa schema, kiểu dữ liệu, deduplicate và ghi lại Silver.
3. **Serving:** nạp Silver vào staging PostgreSQL, normalize numeric/date và upsert vào các bảng serving.

`full` chạy lần lượt:

```text
Bronze → Silver → Serving
```

---

## 2. Nguồn dữ liệu

Nguồn chính là website Cophieu68:

```text
https://cophieu68.vn
```

Các nhóm endpoint được cấu hình trong [cophieu68_config.yaml](flows/cophieu68_deploy_full_pipeline/ingestion/config/cophieu68_config.yaml):

| Nhóm dữ liệu | Endpoint |
|---|---|
| Company profile | `/quote/profile.php?id={symbol}` |
| Financial summary | `/quote/summary.php?id={symbol}` |
| Financial ratios | `/quote/financial.php?id={symbol}` |
| Quarterly financial detail | `/quote/financial_detail.php?id={symbol}&type=quarter` |
| Yearly financial detail | `/quote/financial_detail.php?id={symbol}&type=year` |
| Trading history | `/quote/history.php?cP={page}&id={symbol}` |
| Market list | `/market/markets.php` |
| Industry sectors | `/category/category_index.php`, `/category/category_financial.php` |
| Company industry sector | `/market/markets.php?id={industry_code}` |
| Company market type sector | `/market/markets.php?id={market_type_code}` |

Crawler sử dụng HTTP/HTML parsing với:

- `requests`, `httpx`, `aiohttp`;
- `BeautifulSoup`;
- `pandas.read_html` với `lxml`;
- retry tối đa và backoff được cấu hình trong YAML;
- HTTP delay/timeout để tránh gọi nguồn quá dày.

---

## 3. Công nghệ sử dụng

### Data processing

- **Python 3.11:** runtime chính.
- **Polars 1.12:** xử lý DataFrame và transformation Silver.
- **PyArrow 17:** chuyển đổi Arrow và ghi Parquet dataset.
- **pandas 2.2:** hỗ trợ parsing HTML và một số ingestion helper.
- **NumPy:** xử lý dữ liệu số.

### Storage

- **MinIO:** object storage tương thích S3 cho Bronze/Silver Parquet.
- **s3fs:** filesystem adapter để kết nối PyArrow với MinIO.
- **Apache Parquet:** định dạng lưu trữ columnar.
- **PostgreSQL 15:** staging và serving/Gold.
- **psycopg2/asyncpg/SQLAlchemy:** kết nối PostgreSQL.

### Orchestration and operations

- **Prefect 2:** server/worker orchestration trong Docker Compose.
- **Docker Compose:** khởi chạy local stack.
- **PySpark 3.5:** có sẵn cho các workload Spark mở rộng; flow Cophieu68 hiện dùng Polars.
- **PyYAML / python-dotenv:** cấu hình YAML và environment variables.
- **structlog / colorlog:** logging.

### Không sử dụng trong pipeline này

- dbt không nằm trong runtime flow hiện tại.
- Phase Gold cũ đã được hợp nhất vào Serving PostgreSQL.

---

## 4. Cấu trúc thư mục quan trọng

```text
.
├── cli/main.py
├── flows/
│   ├── common/
│   │   ├── base_config.py
│   │   ├── base_executor.py
│   │   └── base_orchestrator.py
│   └── cophieu68_deploy_full_pipeline/
│       ├── bronze.py
│       ├── silver.py
│       ├── serving.py
│       ├── run.py
│       ├── pipeline_config.py
│       ├── schema/schema_registry.py
│       └── ingestion/config/cophieu68_config.yaml
├── platforms/
│   ├── processing/polars/polars_engine.py
│   └── storage/postgre/
├── infra/
│   ├── Dockerfile
│   ├── docker_compose.yml
│   └── sql/001_init_schemas.sql
├── sql/migrations/
├── shared/logger/
├── requirements.txt
└── export_task/
```

Vai trò chính:

- [cli/main.py](cli/main.py): entrypoint command line.
- [bronze.py](flows/cophieu68_deploy_full_pipeline/bronze.py): ingestion, DQ và raw write.
- [silver.py](flows/cophieu68_deploy_full_pipeline/silver.py): Polars transformations.
- [serving.py](flows/cophieu68_deploy_full_pipeline/serving.py): PostgreSQL loading và normalization.
- [schema_registry.py](flows/cophieu68_deploy_full_pipeline/schema/schema_registry.py): schema registry cho Bronze/Silver/Serving.
- [polars_engine.py](platforms/processing/polars/polars_engine.py): local/S3 Parquet I/O.
- [docker_compose.yml](infra/docker_compose.yml): local infrastructure stack.

---

## 5. Yêu cầu trước khi chạy

### Chạy local

- Linux/macOS/WSL hoặc môi trường tương đương.
- Python 3.11.
- Docker Engine và Docker Compose plugin.
- Có file `.env` tại project root.
- Có quyền truy cập Internet tới `cophieu68.vn`.

### Chạy trong Docker

Dockerfile yêu cầu file `.env` tồn tại tại root vì image hiện copy file này vào `/app/.env`:

```dockerfile
COPY .env .env
```

Không commit `.env` lên Git. File này chứa credentials và các cấu hình môi trường.

---

## 6. Cấu hình environment

Các biến quan trọng:

```dotenv
ENVIRONMENT=prod
PROJECT_NAME=dataops_webcraw
PROJECT_VERSION=1.0

# PostgreSQL khi pipeline chạy trong Docker network
POSTGRES_HOST=xxxx
POSTGRES_PORT=xxxx
POSTGRES_DB=xxxx
POSTGRES_USER=xxxx
POSTGRES_PASSWORD=<your-postgres-password>

# MinIO khi pipeline chạy trong Docker network
S3_ENDPOINT=http://minio:9000
AWS_ACCESS_KEY_ID=<your-minio-access-key>
AWS_SECRET_ACCESS_KEY=<your-minio-secret-key>
LAKEHOUSE_BASE_PATH=s3://<your name>

# Prefect khi pipeline/worker chạy trong Docker network
PREFECT_API_URL=http://prefect-server:4200/api
PREFECT_HOME=/tmp/prefect
```

Giá trị mặc định của stack trong [docker_compose.yml](infra/docker_compose.yml) hiện là:

```text
MinIO endpoint:       http://minio:9000
MinIO console:        http://localhost:9001
MinIO user:            <<admin>>
MinIO password:       <<pass>>

PostgreSQL host:      
PostgreSQL port:      
PostgreSQL database:  
PostgreSQL user:      
PostgreSQL password:  

Prefect API:           http://prefect-server:4200/api
```

Đối với pipeline chạy **bên trong container**, không dùng `localhost` cho MinIO/PostgreSQL/Prefect. Dùng service name trên Docker network:

```text
minio
postgres-local
prefect-server
```

Đối với pipeline chạy **trực tiếp trên host**, có thể dùng:

```dotenv
S3_ENDPOINT=http://localhost:9000
POSTGRES_HOST=localhost
PREFECT_API_URL=http://localhost:4200/api
```

---

## 7. Cài dependency trên host

Từ project root:

```bash
python3.11 -m venv dataops_webcraw_env
source dataops_webcraw_env/bin/activate

python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
```

Kiểm tra import cơ bản:

```bash
python -c "import polars, pyarrow, s3fs, psycopg2, yaml; print('dependencies-ok')"
```

Kiểm tra syntax:

```bash
python -m compileall -q cli flows platforms shared
```

---

## 8. Khởi chạy Docker infrastructure

Tất cả lệnh sau chạy từ project root.

### Build image pipeline

```bash
docker compose -f infra/docker_compose.yml build
```

Build riêng image pipeline:

```bash
docker compose -f infra/docker_compose.yml build pipeline-runner prefect-worker
```

### Start toàn bộ stack

```bash
docker compose -f infra/docker_compose.yml up -d
```

Các service chính:

| Service | Container | Port/địa chỉ |
|---|---|---|
| Prefect server | `prefect-server` | `http://localhost:4200` |
| Prefect worker | `prefect-worker` | Docker network |
| MinIO API | `minio-webcraw` | `http://localhost:9000` |
| MinIO Console | `minio-webcraw` | `http://localhost:9001` |
| PostgreSQL | `postgres-webcraw` | `localhost:5432` |
| Pipeline runner | `pipeline-runner` | Docker network |
| Spark master UI | `spark-webcraw` | `http://localhost:8080` |

### Kiểm tra service

```bash
docker compose -f infra/docker_compose.yml ps
```

Kiểm tra Prefect:

```bash
curl http://localhost:4200/api/health
```

Kiểm tra MinIO:

```bash
curl http://localhost:9000/minio/health/live
```

Kiểm tra PostgreSQL:

```bash
docker compose -f infra/docker_compose.yml exec postgres-local \
  pg_isready -U 'postgres@user' -d dataops_webcraw
```

Xem log infrastructure:

```bash
docker compose -f infra/docker_compose.yml logs -f prefect-server
docker compose -f infra/docker_compose.yml logs -f minio
docker compose -f infra/docker_compose.yml logs -f postgres-local
```

---

## 9. Chạy pipeline trong Docker

### Chạy toàn bộ ELT cho một symbol

```bash
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 full \
  --backend polars \
  --env prod \
  --symbols HPG
```

### Chạy nhiều symbol

```bash
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 full \
  --backend polars \
  --env prod \
  --symbols HPG FPT VNM MBB SSI
```

### Chạy theo ngày cụ thể

```bash
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 full \
  --backend polars \
  --env prod \
  --date 2026-09-16 \
  --symbols HPG
```

### Chạy từng phase

Bronze:

```bash
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 bronze \
  --backend polars --env prod --symbols HPG
```

Silver:

```bash
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 silver \
  --backend polars --env prod --symbols HPG
```

Serving:

```bash
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 serving \
  --backend polars --env prod --symbols HPG
```

Validate configuration mà không ghi dữ liệu:

```bash
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 validate \
  --backend polars --env prod
```

Dry run:

```bash
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 full \
  --backend polars \
  --env prod \
  --symbols HPG \
  --dry-run
```

### Chạy bằng container `pipeline-runner` đang chạy

Nếu stack đã được start bằng `up -d`, có thể dùng:

```bash
docker compose -f infra/docker_compose.yml exec pipeline-runner \
  python -m cli.main cophieu68 full \
  --backend polars \
  --env prod \
  --symbols HPG
```

---

## 10. CLI options

Syntax:

```bash
python -m cli.main PIPELINE PHASE [OPTIONS]
```

Pipeline hiện có:

```text
cophieu68
```

Phase sử dụng trong kiến trúc hiện tại:

```text
bronze
silver
serving
full
validate
```

Options:

| Option | Ý nghĩa |
|---|---|
| `--symbols HPG FPT` | Danh sách mã cổ phiếu |
| `--date YYYY-MM-DD` | Ngày chạy; mặc định là ngày hiện tại |
| `--env prod` | Environment: `prod`, `dev`, `staging` |
| `--backend polars` | Backend hiện dùng |
| `--config PATH` | YAML config path tùy chỉnh |
| `--output summary` | Output: `summary`, `text`, `json` |
| `--dry-run` | Validate mà không mutate storage |

Backend `dbt` và phase `gold` vẫn có thể xuất hiện trong parser tương thích ngược, nhưng không phải execution path được hỗ trợ trong kiến trúc hiện tại. Hãy dùng `--backend polars` và phase `serving`.

---

## 11. Kiểm tra dữ liệu sau khi chạy

### Kiểm tra object trên MinIO

Truy cập MinIO Console:

```text
http://localhost:9001
```

Bucket mặc định:

```text
dataops-lake
```

Các prefix chính:

```text
bronze/<table>/
silver/<table>/
```

### Kiểm tra PostgreSQL

Kết nối từ host:

```bash
psql \
  -h localhost \
  -p 5432 \
  -U 'postgres@user' \
  -d dataops_webcraw
```

Ví dụ kiểm tra schema:

```sql
\dn
\dt staging.*
\dt serving.*
```

Hoặc chạy nhanh từ container:

```bash
docker compose -f infra/docker_compose.yml exec postgres-local \
  psql -U 'postgres@user' -d dataops_webcraw \
  -c '\dt staging.*'
```

### Kiểm tra log pipeline

Log được mount từ host vào container:

```text
logs/
```

Theo dõi log:

```bash
tail -f logs/etl/etl.log
tail -f logs/serving/serving.log
```

Nếu chưa có file log tương ứng, kiểm tra:

```bash
find logs -maxdepth 3 -type f -print
```

Các tín hiệu cần theo dõi:

```text
SilverExecutor errors=0
ServingExecutor errors=0
pipeline status=SUCCESS
```

---

## 12. Database initialization và persistence

PostgreSQL container mount:

```text
infra/sql → /docker-entrypoint-initdb.d
```

Schema initialization chính:

- [001_init_schemas.sql](infra/sql/001_init_schemas.sql)

Migration/reference SQL:

- [V1__create_schemas.sql](sql/migrations/V1__create_schemas.sql)
- [V2__create_staging_tables.sql](sql/migrations/V2__create_staging_tables.sql)
- [V3__create_normalized_tables.sql](sql/migrations/V3__create_normalized_tables.sql)

PostgreSQL và MinIO sử dụng named volumes:

```text
pg_data
minio_data
```

Do đó dữ liệu vẫn tồn tại sau `docker compose down`.

> Lưu ý: các script trong `/docker-entrypoint-initdb.d` chỉ tự chạy khi PostgreSQL volume được khởi tạo lần đầu. Với database đã tồn tại, dùng migration/DDL phù hợp; không kỳ vọng `CREATE TABLE IF NOT EXISTS` tự thay đổi schema cũ.

---

## 13. Dừng và reset stack

Dừng container nhưng giữ dữ liệu:

```bash
docker compose -f infra/docker_compose.yml down
```

Dừng và xóa volumes, sẽ xóa dữ liệu PostgreSQL/MinIO local:

```bash
docker compose -f infra/docker_compose.yml down -v
```

Build lại image không dùng cache:

```bash
docker compose -f infra/docker_compose.yml build --no-cache pipeline-runner prefect-worker
```

---

## 14. Troubleshooting

### `Not a regular file: s3://.../`

Đây là lỗi dùng file writer cho directory dataset. Kiểm tra:

- `S3_ENDPOINT` có trỏ tới `http://minio:9000` khi chạy trong Docker.
- bucket `dataops-lake` có thể được tạo bởi writer.
- container pipeline có cùng Docker network với MinIO.

### `NoSuchBucket`

Kiểm tra MinIO đang healthy:

```bash
curl http://localhost:9000/minio/health/live
docker compose -f infra/docker_compose.yml logs minio
```

### PostgreSQL connection refused

Kiểm tra:

```bash
docker compose -f infra/docker_compose.yml ps postgres-local
docker compose -f infra/docker_compose.yml logs postgres-local
```

Trong container phải dùng:

```dotenv
POSTGRES_HOST=postgres-local
```

### `invalid input syntax for type numeric`

Serving có normalize các format như:

```text
17.9K
132,000,000
1.4x
8,443 Mi
109,842 Bi
```

Nếu xuất hiện format mới, kiểm tra parser tại [serving.py](flows/cophieu68_deploy_full_pipeline/serving.py), không cast raw TEXT trực tiếp trong SQL.

### Industry info dùng dữ liệu cũ

Industry info không được phép fallback stale khi thiếu partition ngày hiện tại. Kiểm tra log:

```text
has no partition for <date> — skipping stale fallback
```

### Income/balance statement dùng `all-Utf8`

Kiểm tra registry có các key:

```text
income_statement_quarter
income_statement_year
balance_sheet_quarter
balance_sheet_year
```

Các key này được định nghĩa trong [schema_registry.py](flows/cophieu68_deploy_full_pipeline/schema/schema_registry.py).

---

## 15. Development checks

Compile toàn bộ source:

```bash
python -m compileall -q cli flows platforms shared
```

Kiểm tra whitespace/diff:

```bash
git diff --check
```

Compile các module pipeline:

```bash
python -m compileall -q \
  flows/cophieu68_deploy_full_pipeline/bronze.py \
  flows/cophieu68_deploy_full_pipeline/silver.py \
  flows/cophieu68_deploy_full_pipeline/serving.py \
  flows/cophieu68_deploy_full_pipeline/schema/schema_registry.py
```

---

## 16. Data quality và giới hạn hiện tại

- Bronze giữ raw data để có thể trace về nguồn.
- Silver chịu trách nhiệm typed/deduplicated data.
- Staging PostgreSQL giữ `TEXT` để không làm mất raw format.
- Serving normalize numeric/date trước khi ghi typed tables.
- Industry freshness được kiểm soát theo partition target date.
- Pipeline success cần được đánh giá cùng `rows_in`, `rows_out`, warning và null rate.

Các điểm cần theo dõi thêm:

1. `fact_business_plan` từng có tình trạng `in=7, out=0`; cần kiểm tra filter/mapping business plan.
2. Semantics của format `5% # 10%` hiện dùng midpoint `7.5`; cần xác nhận với nghiệp vụ.
3. `Mi`/`Bi` hiện được hiểu lần lượt là million/billion; cần xác nhận convention của nguồn.
4. Nên bổ sung targeted tests cho parser và schema registry khi test runner của project được chuẩn hóa.

---

## 17. Quick start

Nếu `.env` đã được cấu hình cho Docker network, quy trình đầy đủ:

```bash
# 1. Build image
docker compose -f infra/docker_compose.yml build

# 2. Start infrastructure
docker compose -f infra/docker_compose.yml up -d

# 3. Kiểm tra service
docker compose -f infra/docker_compose.yml ps

# 4. Validate pipeline
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 validate \
  --backend polars --env prod

# 5. Chạy ELT cho HPG
docker compose -f infra/docker_compose.yml run --rm pipeline-runner \
  python -m cli.main cophieu68 full \
  --backend polars \
  --env prod \
  --symbols HPG

# 6. Xem log
tail -f logs/etl/etl.log
```

