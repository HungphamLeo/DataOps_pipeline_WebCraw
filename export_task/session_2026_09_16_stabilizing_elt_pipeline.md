# Session Report: Stabilizing ELT Pipeline — Cophieu68

**Date:** 2026-09-16  
**Project:** `DataOps_pipeline_WebCraw`  
**Scope:** Cophieu68 Bronze → Silver → Serving PostgreSQL  
**Backend:** Polars + PyArrow + MinIO/S3 + PostgreSQL  
**Status:** Completed for the requested fixes; runtime follow-up remains recommended

---

## 1. Mục tiêu session

Ổn định pipeline dữ liệu cổ phiếu Cophieu68 sau các lần chạy thực tế với symbol `HPG`, tập trung vào:

1. Bỏ phụ thuộc dbt khỏi pipeline hiện tại.
2. Sửa lỗi ghi Parquet lên MinIO/S3.
3. Sửa schema drift giữa Silver, staging và PostgreSQL serving.
4. Chuẩn hóa numeric/date từ dữ liệu nguồn không đồng nhất.
5. Kiểm soát dữ liệu industry bị fallback về snapshot cũ.
6. Bổ sung schema registry cho income statement và balance sheet.
7. Ghi lại nguyên nhân, cách xử lý và các rủi ro còn lại để làm knowledge base cho các session sau.

Kiến trúc sau khi hoàn thiện:

```text
Crawler
  → Bronze raw Parquet trên MinIO
  → Silver typed/deduplicated Parquet bằng Polars
  → PostgreSQL staging (TEXT/raw-compatible)
  → PostgreSQL serving/Gold (normalized typed tables)
```

Repo này **không dùng dbt**. dbt được dành cho repo khác theo quyết định kiến trúc của người dùng.

---

## 2. Tóm tắt kết quả

Các lỗi chính đã được xử lý:

- Đường dẫn S3 directory bị truyền vào file writer Parquet.
- Partitioned Parquet dataset không ghi được lên MinIO.
- Bucket MinIO chưa tồn tại khi pipeline bắt đầu ghi.
- PostgreSQL staging/serving bị schema drift.
- SQL serving tham chiếu sai tên cột `market_type` và `_ingest_timestamp`.
- Numeric raw như `17.9K`, `132,000,000`, `1.4x`, `8,443 Mi`, `109,842 Bi` không được parse.
- Date dạng `31/07/2026` không tương thích với PostgreSQL date parser.
- Numeric precision của completion rate quá nhỏ.
- Industry info dùng fallback historical data dù thiếu partition ngày hiện tại.
- Income/balance statement bị rơi vào schema all-UTF8 vì key runtime không có trong registry.
- Một số lỗi cấu hình, dependency, logger và PostgreSQL/MinIO credential mismatch từ các lần debug nền tảng trước.

Kết quả run đã ghi nhận trước khi hoàn thiện ba fix cuối:

```text
SilverExecutor errors=0
ServingExecutor errors=0
pipeline status=SUCCESS
tables_synced=140
```

Validation sau các thay đổi cuối:

```text
python3 -m compileall -q \
  flows/cophieu68_deploy_full_pipeline/serving.py \
  flows/cophieu68_deploy_full_pipeline/silver.py \
  flows/cophieu68_deploy_full_pipeline/schema/schema_registry.py

git diff --check
```

Kết quả:

```text
validation-ok
```

---

## 3. Quyết định kiến trúc: loại dbt khỏi repo

### Triệu chứng và vấn đề

Pipeline cũ có các thành phần dbt dù flow thực tế đang xử lý trực tiếp bằng Polars/Python. Điều này tạo thêm dependency, project skeleton và phase không cần thiết.

### Quyết định

Sử dụng trực tiếp:

- Polars cho transformation.
- PyArrow/s3fs cho Parquet và S3/MinIO I/O.
- PostgreSQL serving để lưu các bảng normalized/Gold.

Phase runtime được chuẩn hóa thành:

```python
["bronze", "silver", "serving"]
```

### Thành phần đã thay đổi

- Xóa `GoldExecutor` cũ.
- Xóa `DbtRunner` và dbt factory builder.
- Xóa dbt project skeleton khỏi runtime.
- Xóa `dbt-core` và `dbt-postgres` khỏi `requirements.txt`.
- Giữ Silver transformation bằng Polars.

### Tác động

Pipeline đơn giản hơn và không còn yêu cầu dbt trong runtime của repo này. Các mô hình dbt nếu cần sẽ được triển khai ở repo riêng.

---

## 4. Fix Bronze Parquet trên MinIO/S3

### 4.1. Lỗi `Not a regular file`

#### Log lỗi

```text
Not a regular file: 's3://dataops-lake/bronze/company_profile/'
Not a regular file: 's3://dataops-lake/bronze/financial_ratios/'
```

### Root cause

Đường dẫn kết thúc bằng `/` là directory dataset, nhưng lại được truyền vào file writer. Polars/PyArrow file writer yêu cầu một file cụ thể trong khi partitioned dataset cần dataset writer.

### Cách fix

Trong [polars_engine.py](../platforms/processing/polars/polars_engine.py):

- Nhận diện S3/MinIO path riêng với local path.
- Partitioned dataset dùng `pyarrow.dataset.write_dataset`.
- Non-partitioned S3 output dùng `pyarrow.parquet.write_table`.
- Convert Polars DataFrame sang PyArrow Table bằng `df.to_arrow()`.
- Local path tiếp tục dùng Polars.
- Chuẩn hóa path file/directory trước khi ghi.

### 4.2. Lỗi bucket chưa tồn tại

#### Log lỗi

```text
NoSuchBucket: The specified bucket does not exist
```

### Root cause

` s3fs ` không tự tạo bucket trong mọi trường hợp ghi dataset.

### Cách fix

Writer kiểm tra bucket và tạo bucket trước khi ghi:

```text
ensure bucket exists
→ write dataset/file
```

### Kết quả

Bronze đã ghi được lên các path dạng:

```text
s3://dataops-lake/bronze/<table>/
s3://dataops-lake/silver/<table>/
```

---

## 5. Fix cấu hình S3/MinIO và dependency

### 5.1. `use_ssl` hoặc S3 filesystem kwarg không tương thích

#### Log lỗi

```text
__init__() got an unexpected keyword argument 'use_ssl'
AWS Error INVALID_ACCESS_KEY_ID during CreateMultipartUpload
```

### Root cause

`storage_options` của Polars bị chuyển không đúng vào `pyarrow.fs.S3FileSystem`. Các option của MinIO như endpoint và credentials không tương thích trực tiếp với constructor đang được sử dụng.

### Cách fix

Không dùng Polars native S3 writer cho flow này. Dùng:

```text
s3fs.S3FileSystem
→ PyArrow dataset/parquet writer
```

Dependency liên quan:

```text
s3fs==2024.6.1
pyarrow
polars
```

### 5.2. PostgreSQL/MinIO credential mismatch

Đã đồng bộ default config với Docker Compose:

| Setting | Giá trị chuẩn |
|---|---|
| `POSTGRES_USER` | `postgres@user` |
| `POSTGRES_PASSWORD` | `password@123` |
| `POSTGRES_DB` | `dataops_webcraw` |

Đã sửa:

- [base_config.py](../flows/common/base_config.py)
- [docker_compose.yml](../infra/docker_compose.yml)
- `.env.example`

Healthcheck PostgreSQL cũng sử dụng đúng user/database.

### 5.3. Thiếu `lxml`

`pandas.read_html(..., flavor="lxml")` fail vì dependency chưa được khai báo. Đã thêm:

```text
lxml==5.3.0
```

### 5.4. `.gitignore` ignore nhầm file cấu hình

Glob quá rộng như `*.yaml`, `*.yml`, `*.txt`, `*.json` làm các file runtime quan trọng bị ignore.

Đã điều chỉnh `.gitignore` để chỉ ignore:

- virtual environment;
- `.env`;
- logs;
- bytecode;
- build artifacts;
- cache.

---

## 6. Fix logger và đường dẫn configuration

### 6.1. Log chỉ xuất ra terminal

### Root cause

Logger runtime dùng tên `etl.*` và `platforms.*`, nhưng YAML chỉ khai báo một số logger tên `logger.*`. Vì vậy log rơi vào root logger và không ghi đúng file.

### Cách fix

Trong `logger_config.yaml`:

- Thêm handler cho ETL và platform.
- Map các logger `etl.*`, `platforms.*`, ingestion, serving về đúng file.
- Tạo các thư mục log cần thiết.

### 6.2. Config path trỏ sai

Config thực tế nằm tại:

```text
flows/cophieu68_deploy_full_pipeline/ingestion/config/cophieu68_config.yml
```

Đã sửa `pipeline_config.py` dùng path tương đối từ `__file__` thay vì hardcode path sai từ project root.

---

## 7. Fix Bronze data quality và metadata

### Lỗi `symbol is null`

#### Triệu chứng

```text
[DQ][HPG] 1/100: symbol is null
...
[DQ][HPG] 100/100: symbol is null
```

### Root cause

`TradingRecord` không có field `symbol`. DQ chạy trước bước inject symbol vào DataFrame.

### Cách fix

Trong bước trading Bronze:

- Inject symbol vào raw records trước khi gọi cleansing/DQ.
- Chuẩn hóa symbol về uppercase.

Nhờ đó rule `symbol_not_null` kiểm tra đúng dữ liệu đầu vào.

---

## 8. Fix PostgreSQL schema drift và serving SQL

### 8.1. `CREATE TABLE IF NOT EXISTS` không migrate bảng cũ

### Root cause

PostgreSQL không cập nhật schema của bảng đã tồn tại khi DDL chỉ dùng `CREATE TABLE IF NOT EXISTS`.

### Cách fix

Trong [serving.py](../flows/cophieu68_deploy_full_pipeline/serving.py):

- Bổ sung `_ensure_table_columns()`.
- Dùng `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- Đảm bảo staging và serving có các column canonical trước khi insert.
- Chỉ insert các column nằm trong schema registry.

### 8.2. Tham chiếu sai cột market type

#### Log lỗi

```text
column "market_type" does not exist
```

### Root cause

Query đọc `market_type` từ staging trong khi staging dùng:

```text
market_type_code
market_type_name
```

### Cách fix

SQL serving đọc đúng staging columns rồi map sang Gold:

```text
market_type_code → market_type
market_type_name → market_name
```

### 8.3. Thiếu `_ingest_timestamp`

#### Log lỗi

```text
column "_ingest_timestamp" does not exist
```

### Cách fix

- Bổ sung `_ingest_timestamp` vào staging DDL/registry.
- Bổ sung migration logic cho database đã tồn tại.
- Cho phép dedup/upsert dùng cột audit này.

### 8.4. Chuẩn hóa business plan columns

Đã chuẩn hóa tên nguồn về canonical snake_case:

```text
Year          → year
Plan_revenue  → plan_revenue
Pass_revenue  → pass_revenue
Plan_profit   → plan_profit
Pass_profit   → pass_profit
```

---

## 9. Fix numeric normalization

### 9.1. Các lỗi ban đầu

```text
invalid input syntax for type numeric: "17.9K"
invalid input syntax for type bigint: "132,000,000"
```

### Root cause

Staging nhận raw string nhưng SQL serving cast trực tiếp sang numeric/bigint. Dữ liệu nguồn có comma và suffix nên PostgreSQL cast thất bại.

### Nguyên tắc xử lý

- Staging tiếp tục giữ `TEXT` để không mất raw input.
- Normalize tại serving boundary trước khi cast vào typed table.
- Null markers được xử lý thành `NULL`.
- Không để một raw format làm hỏng toàn bộ batch.

### Các format đã hỗ trợ

| Raw value | Normalized value |
|---|---:|
| `17.9K` | `17900` |
| `132,000,000` | `132000000` |
| `1.4x` | `1.4` |
| `13.5x` | `13.5` |
| `8,443 Mi` | `8443000000` |
| `109,842 Bi` | `109842000000000` |
| `5% # 10%` | `7.5` |
| `None`, `NULL`, `-`, `N/A` | `NULL` |

Suffix hỗ trợ:

```text
K, M, B, T, Mi, Bi, x
```

### Semantics range

Với format:

```text
5% # 10%
```

parser lấy midpoint:

```text
(5 + 10) / 2 = 7.5
```

Đây là quy tắc kỹ thuật hiện tại để không làm mất metric. Nếu nghiệp vụ yêu cầu lower bound, upper bound hoặc lưu cả hai biên, cần thay bằng schema riêng trong một task sau.

---

## 10. Fix date normalization và numeric precision

### 10.1. Date format không tương thích PostgreSQL

#### Log lỗi

```text
date/time field value out of range: "31/07/2026"
HINT: Perhaps you need a different "datestyle" setting.
```

### Cách fix

Serving normalize các format:

```text
DD/MM/YYYY
DD-MM-YYYY
YYYY-MM-DD
YYYY/MM/DD
```

về ISO:

```text
YYYY-MM-DD
```

Không phụ thuộc `datestyle` của PostgreSQL.

### 10.2. Numeric field overflow

#### Log lỗi

```text
numeric field overflow
precision 8, scale 4 must round to an absolute value less than 10^4
```

### Cách fix

Completion rate được đổi từ:

```sql
NUMERIC(8,4)
```

sang:

```sql
NUMERIC(10,4)
```

Đồng thời bổ sung migration logic cho database hiện tại.

Các numeric fields có giá trị lớn được mở rộng trong:

- [001_init_schemas.sql](../infra/sql/001_init_schemas.sql)
- [serving.py](../flows/cophieu68_deploy_full_pipeline/serving.py)

---

## 11. Industry freshness policy

### Vấn đề

`_read_bronze()` trước đây luôn fallback:

```text
bronze/<table>/ingest_date=<target_date>/*.parquet
→ nếu không có thì đọc toàn bộ bronze/<table>/**/*.parquet
```

Điều này có thể làm industry info của ngày hiện tại lấy dữ liệu cũ nhưng pipeline vẫn báo thành công.

### Cách fix

Đã bổ sung tham số:

```python
allow_fallback: bool = True
```

Các bảng industry info gọi:

```python
allow_fallback=False
```

Khi không có partition đúng ngày:

- Không đọc historical snapshot cũ.
- Ghi warning rõ ràng.
- Bỏ qua dữ liệu industry trong run đó.

Log dự kiến:

```text
[Silver] bronze/industry_info_<kind> has no partition for <date> — skipping stale fallback
```

Các bảng khác vẫn giữ fallback mặc định để tránh thay đổi behavior ngoài phạm vi issue.

---

## 12. Schema registry cho income statement và balance sheet

### Vấn đề

Runtime thực tế ghi các table:

```text
income_statement_quarter
income_statement_year
balance_sheet_quarter
balance_sheet_year
```

Registry chỉ có:

```text
income_statement
balance_sheet
```

Do đó Bronze log:

```text
table=<runtime_name> not in schema_registry — using all-Utf8
```

### Cách fix

Trong [schema_registry.py](../flows/cophieu68_deploy_full_pipeline/schema/schema_registry.py), bổ sung aliases:

```python
"income_statement_quarter": BRONZE_INCOME_STATEMENT
"income_statement_year": BRONZE_INCOME_STATEMENT
"balance_sheet_quarter": BRONZE_BALANCE_SHEET
"balance_sheet_year": BRONZE_BALANCE_SHEET
```

Các table runtime này tiếp tục dùng:

- schema Bronze đã định nghĩa;
- partition theo `ingest_date`;
- các audit columns và metric columns canonical.

---

## 13. Các file/module liên quan

### Pipeline execution

- [run.py](../flows/cophieu68_deploy_full_pipeline/run.py): điều phối Bronze → Silver → Serving.
- [bronze.py](../flows/cophieu68_deploy_full_pipeline/bronze.py): crawl result, metadata injection, DQ và Bronze write.
- [silver.py](../flows/cophieu68_deploy_full_pipeline/silver.py): đọc Bronze, transform bằng Polars, fallback policy.
- [serving.py](../flows/cophieu68_deploy_full_pipeline/serving.py): normalize dữ liệu, schema migration và PostgreSQL write.

### Schema and storage

- [schema_registry.py](../flows/cophieu68_deploy_full_pipeline/schema/schema_registry.py): single source of truth cho Bronze/Silver/Gold schema.
- [polars_engine.py](../platforms/processing/polars/polars_engine.py): local/S3 Parquet I/O.
- [001_init_schemas.sql](../infra/sql/001_init_schemas.sql): PostgreSQL initial DDL.
- [V2__create_staging_tables.sql](../sql/migrations/V2__create_staging_tables.sql): migration staging cũ, dùng để đối chiếu naming/schema.

### Configuration and dependencies

- [base_config.py](../flows/common/base_config.py): PostgreSQL defaults và environment config.
- [pipeline_config.py](../flows/cophieu68_deploy_full_pipeline/pipeline_config.py): pipeline config path và runtime settings.
- [requirements.txt](../requirements.txt): Polars, PyArrow, s3fs, lxml và PostgreSQL dependencies.
- [docker_compose.yml](../infra/docker_compose.yml): local PostgreSQL/MinIO services.

---

## 14. Validation đã thực hiện

### Static validation

Đã chạy thành công:

```bash
python3 -m compileall -q \
  flows/cophieu68_deploy_full_pipeline/serving.py \
  flows/cophieu68_deploy_full_pipeline/silver.py \
  flows/cophieu68_deploy_full_pipeline/schema/schema_registry.py

git diff --check
```

### Runtime evidence

Run trước khi áp dụng ba fix cuối đã đạt:

```text
SilverExecutor errors=0
ServingExecutor errors=0
pipeline status=SUCCESS
tables_synced=140
```

### Chưa thực hiện trong môi trường hiện tại

Chưa có runtime integration run mới sau đúng ba thay đổi cuối do môi trường shell hiện tại không có đầy đủ virtualenv/runtime packages để chạy toàn bộ pipeline. Vì vậy cần xác nhận tiếp bằng run thực tế.

---

## 15. Hạng mục còn mở và rủi ro

### 15.1. Cần chạy lại HPG sau patch

Lệnh đề xuất:

```bash
source dataops_webcraw_env/bin/activate
python3 -m cli.main cophieu68 full \
  --backend polars \
  --env prod \
  --symbols HPG
```

Cần kiểm tra:

- Không còn `schema=fallback` cho income/balance statement.
- Numeric `x`, `Mi`, `Bi`, range không bị chuyển thành `NULL`.
- Industry info không lấy snapshot cũ.
- Bronze, Silver, Serving đều có `errors=0`.

### 15.2. `fact_business_plan` có dấu hiệu lọc hết dữ liệu

Log trước đó:

```text
Loaded 7 rows → staging.fact_business_plan
fact_business_plan: in=7 out=0
```

Đây là issue chưa nằm trong ba fix được ưu tiên ở session này. Cần kiểm tra:

- raw `year`;
- `plan_key`;
- filter điều kiện trong serving;
- mapping giữa schema registry và SQL upsert.

### 15.3. Semantics range phần trăm

Hiện `5% # 10%` dùng midpoint `7.5`. Cần xác nhận với business xem có cần:

- lower bound;
- upper bound;
- midpoint;
- hai cột riêng;
- hoặc giữ raw string và thêm metric parsed.

### 15.4. Semantics `Mi`/`Bi`

Parser đang hiểu:

```text
Mi = million
Bi = billion
```

Cần xác nhận lại với convention của nguồn Cophieu68 nếu các suffix này có nghĩa domain khác.

### 15.5. Dedup income/balance statement

Silver hiện cần được tiếp tục review để bảo đảm dedup key bao gồm đầy đủ:

```text
symbol
report_type
period
```

Tránh mất record giữa các kỳ báo cáo.

---

## 16. Lessons learned

1. Directory Parquet dataset và single Parquet file phải đi qua hai writer khác nhau.
2. Staging nên giữ raw-compatible `TEXT`; normalize tại serving boundary.
3. `CREATE TABLE IF NOT EXISTS` không phải schema migration.
4. Registry key phải khớp tên table runtime thực tế, không chỉ khớp tên logical.
5. Fallback historical data cần policy rõ ràng theo từng loại bảng; snapshot dimension không nên mặc định fallback mù.
6. Numeric parser cần được thiết kế theo dữ liệu thực tế của source, không chỉ theo kiểu PostgreSQL đích.
7. Pipeline success về orchestration không đồng nghĩa dữ liệu không bị mất; cần theo dõi `rows_in`, `rows_out`, null rate và warning.
8. Mọi config mặc định phải khớp Docker Compose và `.env.example`.
9. Logger name runtime phải được map chính xác trong logger configuration.
10. Mỗi session nên để lại report trong `export_task` để các session sau có thể tiếp tục từ evidence thay vì đoán lại nguyên nhân.

---

## 17. Checklist cho session kế tiếp

- [ ] Chạy full pipeline HPG bằng virtualenv đầy đủ.
- [ ] Xác nhận parser với các mẫu `1.4x`, `8,443 Mi`, `109,842 Bi`, `5% # 10%`.
- [ ] Kiểm tra không còn log registry fallback cho income/balance.
- [ ] Kiểm tra industry freshness theo target date.
- [ ] Điều tra `fact_business_plan: in=7, out=0`.
- [ ] Kiểm tra null rate sau serving normalization.
- [ ] Xác nhận semantics business của range phần trăm và suffix `Mi`/`Bi`.
- [ ] Bổ sung targeted tests nếu project đã có test runner phù hợp.

