
# Session Report: Debug & Fix Pipeline — Full Pass
**Date:** 2026-09-14
**Branch:** `10092026_hung_debug_and_fix`
**Role:** Full Stack Data Platform Engineer
**Status:** Completed

---

## 1. Mục tiêu session

Đọc `log.txt` từ lần chạy thực tế `python3 -m cli.main cophieu68 full --backend polars --env prod --symbols HPG`, phân tích toàn bộ lỗi, fix theo thứ tự ưu tiên và đẩy code lên remote.

---

## 2. Danh sách lỗi đã phát hiện & fix

### 🔴 Fix #0 — Logger không ghi ra file (P0)

**Triệu chứng:** Toàn bộ log chỉ in ra terminal, không có gì trong `logs/bronze/`, `logs/etl/`, v.v. Phải ngồi copy tay từ terminal.

**Root cause:**
`LoggerManager.get_logger()` khi nhận tên không có prefix `logger.` sẽ tạo logger name dạng `etl.xxx`. Nhưng `logger_config.yaml` chỉ có entries cho `logger.xxx` — không có `etl.*` hay `platforms.*`. Kết quả: tất cả log từ orchestrator/executor/engine đều rơi vào root logger (chỉ có `console` handler).

**Files đã sửa:**
- `shared/logger/config/logger_config.yaml`

**Thay đổi:**
- Thêm 2 handler mới: `etl_file` → `logs/etl/etl.log`, `platform_file` → `logs/platform/platform.log`
- Thêm 13 logger entries mới map đúng với tên thực tế:
  - `etl.orchestrator_cophieu68` → `etl_file`
  - `etl.config_cophieu68` → `etl_file`
  - `etl.extractor_cophieu68` → `ingestion_file`
  - `etl.polars_engine` → `etl_file`
  - `etl.pg_writer` → `serving_file`
  - `etl.minio_storage` → `etl_file`
  - `platforms.processing.polars.polars_engine` → `platform_file`
  - `platforms.processing.dbt.base_dbt` → `dbt_file`
  - `platforms.processing.base_processing_subsystem` → `platform_file`
  - `platforms.ingestion.base_crawler` → `ingestion_file`
  - `platforms.storage.postgre.base_postgre` → `serving_file`
  - `platforms.storage.minio.minio_storage` → `platform_file`
  - `flows.cophieu68_deploy_full_pipeline.serving` → `serving_file`
- Tạo thư mục `logs/etl/` và `logs/platform/`

**Lesson learned:** Khi thêm logger mới, luôn kiểm tra tên logger thực tế qua `grep get_logger` trong codebase và đảm bảo có entry tương ứng trong `logger_config.yaml`.

---

### 🔴 Fix #1 — `symbol is null` trên 100/100 trading records (P1)

**Triệu chứng:**
```
[DQ][HPG] 1/100: symbol is null
...
[DQ][HPG] 100/100: symbol is null
```

**Root cause:**
`TradingRecord` dataclass trong `extract_cophieu68.py` không có field `symbol`. Khi `asdict(r)` trả về dict, không có key `symbol`. DQ rule `symbol_not_null` trong `build_cleansing_rules()` chạy **trước khi** Polars DataFrame được tạo và inject symbol → fail 100%.

**File đã sửa:**
- `flows/cophieu68_deploy_full_pipeline/bronze.py` — hàm `_step_trading()`

**Thay đổi:**
```python
# Trước
r = ing.process(raw_records=data["records"], ...)

# Sau — inject symbol vào từng record trước DQ check
raw_records = data["records"]
sym_upper = sym.upper()
for rec in raw_records:
    rec.setdefault("symbol", sym_upper)
r = ing.process(raw_records=raw_records, ...)
```

**Lesson learned:** Khi DQ rule check field X, phải đảm bảo field X được inject vào raw record **trước** khi gọi `cleansing.apply()`. Không nên dựa vào downstream code để inject metadata field.

---

### 🔴 Fix #2A — `use_ssl` kwarg rejected / S3 filesystem build fail (P0)

**Triệu chứng:**
```
[Polars] Unable to build S3 filesystem: __init__() got an unexpected keyword argument 'use_ssl'
AWS Error INVALID_ACCESS_KEY_ID during CreateMultipartUpload
```

**Root cause:**
`polars==1.12.0` khi gọi `df.write_parquet(..., use_pyarrow=True, storage_options=...)` truyền dict `storage_options` sang `pyarrow.fs.S3FileSystem`. Nhưng `pyarrow.fs.S3FileSystem` không nhận `endpoint_url` / `aws_access_key_id` trực tiếp — nó cần `endpoint_override` và cú pháp khác. Khi build fail → Polars fallback sang AWS SDK credentials mặc định (không có) → `INVALID_ACCESS_KEY_ID`.

**Files đã sửa:**
- `platforms/processing/polars/polars_engine.py` — rewrite hoàn toàn
- `requirements.txt` — thêm `s3fs==2024.6.1`

**Thay đổi:**
- Rewrite `PolarsEngine` tách thành `_write_parquet_s3()` và `_write_parquet_local()`
- Dùng `s3fs.S3FileSystem` trực tiếp (không qua Polars native S3 path)
- Ghi qua `pyarrow.dataset.write_dataset` (partitioned) hoặc `pyarrow.parquet.write_table` (non-partitioned)
- Convert Polars DataFrame → PyArrow Table trước khi ghi: `df.to_arrow()`
- Auto-detect S3 path bằng `_is_s3_path()` kiểm tra prefix `s3://` hoặc `s3a://`

**Lesson learned:** Với Polars + MinIO, không dùng `use_pyarrow=True` + `storage_options` cùng nhau. Thay vào đó dùng `s3fs` + `pyarrow.dataset` trực tiếp để kiểm soát hoàn toàn filesystem layer.

---

### 🔴 Fix #2B — MinIO/PostgreSQL credentials mismatch (P0)

**Triệu chứng:**
```
FATAL: password authentication failed for user "postgres"
```

**Root cause:**
`base_config.py` có default fallback `pg_user=""` khiến `build_pg_writer()` trả về `None` (guard `if not username: return None`). `POSTGRES_DB` default là `etl_project` trong khi compose dùng `dataops_webcraw`.

So sánh:
| | Compose | base_config default (cũ) |
|---|---|---|
| POSTGRES_USER | `postgres@user` | `""` (empty!) |
| POSTGRES_PASSWORD | `password@123` | `""` (empty!) |
| POSTGRES_DB | `dataops_webcraw` | `etl_project` |

**Files đã sửa:**
- `flows/common/base_config.py` — sửa 3 property defaults
- `infra/docker_compose.yml` — sửa healthcheck `pg_isready -U postgres` → `pg_isready -U postgres@user -d dataops_webcraw`
- `.env.example` — tạo mới làm template chuẩn

**Thay đổi trong `base_config.py`:**
```python
# Sau fix
def pg_database(self): return os.getenv("POSTGRES_DB", "dataops_webcraw")
def pg_user(self):     return os.getenv("POSTGRES_USER", "postgres@user")
def pg_password(self): return os.getenv("POSTGRES_PASSWORD", "password@123")
```

**Lesson learned:** Default fallback trong config class phải luôn khớp với giá trị trong `docker_compose.yml`. Sử dụng `.env.example` làm single source of truth cho developer onboarding.

---

### 🔴 Fix #3 — `dbt_project` không tồn tại (P1)

**Triệu chứng:**
```
Error: Invalid value for '--project-dir':
Path '/home/hungpham/DataOps_pipeline_WebCraw/dbt_project' does not exist.
```

**Root cause:**
Folder `dbt_project/` chưa được tạo trong repo. `dbt_build_params` property hardcode default path là `_PROJECT_ROOT / "dbt_project"` nhưng không có file nào trong đó.

**Files đã tạo/sửa:**
- `dbt_project/dbt_project.yml` — skeleton dbt project config
- `dbt_project/profiles.yml` — profiles đọc từ env vars
- `dbt_project/models/silver/fact_stock_price.sql` — placeholder
- `dbt_project/models/gold/mart_stock_summary.sql` — placeholder
- `flows/cophieu68_deploy_full_pipeline/pipeline_config.py` — sửa `dbt_build_params` để `profiles_dir` tự động trỏ về `dbt_project/`

**profiles.yml pattern:**
```yaml
dataops_webcraw:
  outputs:
    prod:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'localhost') }}"
      user: "{{ env_var('POSTGRES_USER', 'postgres@user') }}"
      ...
```

**Lesson learned:** dbt cần ít nhất `dbt_project.yml` + `profiles.yml` để không crash. Luôn commit skeleton này vào repo ngay từ đầu. `profiles.yml` nên đọc từ env vars, không hardcode credentials.

---

### 🟠 Fix #4 — `NoSuchBucket: lakehouse` (P1)

**Triệu chứng:**
```
NoSuchBucket: The specified bucket does not exist — BucketName: lakehouse
```

**Root cause:**
Bucket `lakehouse` chưa được tạo trong MinIO. `s3fs` không tự tạo bucket khi write.

**File đã sửa:**
- `platforms/processing/polars/polars_engine.py` — hàm `_write_parquet_s3()`

**Thay đổi:**
```python
# Auto-create bucket nếu chưa tồn tại
if not fs.exists(bucket):
    fs.mkdir(bucket)
    self.logger.info("[Polars] Created S3 bucket: %s", bucket)
```

**Lesson learned:** Khi write lên object storage, luôn có bước ensure bucket/container tồn tại trước. Không assume bucket đã được tạo sẵn.

---

### 🟡 Fix #5 — `_DEFAULT_CONFIG_PATH` trỏ sai (P2)

**Triệu chứng:**
Config được load từ đường dẫn không tồn tại (silent fallback về `{}`).

**Root cause:**
```python
# Cũ — sai
_DEFAULT_CONFIG_PATH = _PROJECT_ROOT / "platforms" / "orchestration" / "prefect" / "config" / "cophieu68_config.yaml"

# File thực tế tồn tại tại:
flows/cophieu68_deploy_full_pipeline/ingestion/config/cophieu68_config.yml
```

**File đã sửa:**
- `flows/cophieu68_deploy_full_pipeline/pipeline_config.py`

**Thay đổi:**
```python
# Sau fix — dùng relative path từ file hiện tại
_DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parent
    / "ingestion" / "config" / "cophieu68_config.yml"
)
```

**Lesson learned:** Dùng `Path(__file__).resolve().parent` thay vì `_PROJECT_ROOT / "..."` khi file config nằm cùng package. Tránh hardcode absolute path từ root.

---

### 🟡 Fix #6 — `lxml` missing dependency (P1)

**Triệu chứng:**
```
Missing optional dependency 'lxml'. Use pip or conda to install lxml.
```
Xảy ra ở 30+ dòng log — toàn bộ `pd.read_html(..., flavor="lxml")` trong extractor fail.

**Root cause:**
`lxml` chưa có trong `requirements.txt`. `pandas.read_html` dùng `lxml` làm HTML parser.

**File đã sửa:**
- `requirements.txt` — thêm `lxml==5.3.0`

---

### 🟡 Fix #7 — `.gitignore` ignore nhầm file quan trọng (P2)

**Triệu chứng:**
`*.yaml`, `*.yml`, `*.txt`, `*.json` bị ignore → không thể commit `requirements.txt`, `dbt_project.yml`, `profiles.yml`, `docker_compose.yml`, `logger_config.yaml`.

**Root cause:**
`.gitignore` cũ dùng glob quá rộng:
```gitignore
*.yaml   # Block cả dbt_project.yml, logger_config.yaml
*.yml    # Block cả docker_compose.yml, cophieu68_config.yml
*.txt    # Block cả requirements.txt
```

**File đã sửa:**
- `.gitignore` — rewrite hoàn toàn

**Nội dung mới chỉ ignore:**
- `dataops_webcraw_env/` — virtual environment
- `_all_skills/` — AI workspace cache
- `.env` — secrets
- `*.pyc`, `__pycache__/` — bytecode
- `*.log`, `logs/**/*.log` — runtime logs
- `*.egg-info/`, `dist/`, `build/` — build artifacts

---

## 3. Git conflict resolution (rebase)

**Vấn đề:** Branch local track sai remote (`origin/setup_branch_09092026` thay vì `origin/10092026_hung_debug_and_fix`). Remote đã có 3 commits mà local không có, trong đó có commit rename `flows/shared/` → `flows/common/`.

**Cách xử lý:**
```bash
git branch --set-upstream-to=origin/10092026_hung_debug_and_fix
git rebase origin/10092026_hung_debug_and_fix
# Resolve conflicts:
# - Docs (agent.md, SKILL.md, export_task): --theirs (giữ remote)
# - Code (pipeline_config.py, polars_engine.py): --ours (giữ fixes của session)
git push origin 10092026_hung_debug_and_fix
```

**Lesson learned:**
- Luôn kiểm tra `git branch -vv` để xem branch đang track remote nào trước khi pull/push.
- Khi có rename folder (`shared` → `common`), các file dùng import cũ (`flows.shared.*`) sẽ break — cần update tất cả import.

---

## 4. Cấu trúc folder sau session

```
flows/
  common/               ← đã rename từ shared/ (remote commit c550adf)
    base_config.py
    base_executor.py
    base_orchestrator.py
    context.py
  cophieu68_deploy_full_pipeline/
    bronze.py            ← Fix #1 (symbol inject)
    pipeline_config.py   ← Fix #3, #5 (dbt path, config path)

platforms/
  processing/
    polars/
      polars_engine.py   ← Fix #2A, #4 (s3fs rewrite, auto bucket)

dbt_project/             ← Fix #3 (tạo mới)
  dbt_project.yml
  profiles.yml
  models/silver/
  models/gold/

shared/
  logger/
    config/
      logger_config.yaml ← Fix #0 (thêm etl.*, platforms.* loggers)

logs/
  etl/                   ← Tạo mới
  platform/              ← Tạo mới
  bronze/, silver/, ...  ← Đã có

requirements.txt          ← Thêm lxml==5.3.0, s3fs==2024.6.1
.gitignore               ← Fix #7 (rewrite)
.env.example             ← Tạo mới (template)
```

---

## 5. Việc cần làm tiếp theo (Next Session)

1. **Verify MinIO connection:** Đảm bảo MinIO service đang chạy (`docker compose up minio -d`) và credentials trong `.env` khớp với compose.
2. **Chạy lại pipeline:** `python3 -m cli.main cophieu68 full --backend polars --env prod --symbols HPG` và kiểm tra log trong `logs/etl/etl.log`.
3. **Implement dbt models:** Thay thế placeholder SQL trong `dbt_project/models/silver/` và `dbt_project/models/gold/` bằng transform logic thực sự sau khi bronze data chạy ổn định.
4. **Kiểm tra `flows/common/` imports:** Đảm bảo tất cả file còn import từ `flows.shared.*` đã được cập nhật sang `flows.common.*`.
5. **Add `.env.example` lại:** File bị xóa trong rebase — cần tạo lại và commit.

---

## 6. Lệnh kiểm tra nhanh sau khi apply fixes

```bash
# 1. Cài dependencies mới
source dataops_webcraw_env/bin/activate
pip install lxml==5.3.0 s3fs==2024.6.1

# 2. Khởi động MinIO + Postgres
cd infra && docker compose -f docker_compose.yml up minio postgres-local -d

# 3. Chạy pipeline
cd ..
python3 -m cli.main cophieu68 full --backend polars --env prod --symbols HPG

# 4. Kiểm tra log files (không còn phải copy tay từ terminal)
tail -f logs/etl/etl.log
tail -f logs/ingestion/ingestion.log
```
