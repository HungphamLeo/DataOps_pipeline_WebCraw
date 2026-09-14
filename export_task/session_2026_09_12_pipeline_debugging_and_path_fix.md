# Session Report: Pipeline Debugging & Config Path Fix
**Date:** 2026-09-12  
**Role:** Full Stack Data Platform Engineer  
**Status:** In-progress / debugging active

---

## 1. Tóm tắt mục tiêu
Mục tiêu của session hôm nay là ổn định pipeline `cophieu68` chạy trên stack hiện tại của repo với engine `polars`, đồng thời đồng bộ config path và lifecycle contract giữa orchestrator và executor.

Các điểm đang được xử lý:
- fix config YAML path bị sai
- fix lifecycle `pre_execute()` / `post_execute()` mismatch giữa orchestrator và executor
- xác định các blocker kỹ thuật thực hiện tiếp theo: dbt project missing, Polars write_parquet API mismatch, Postgres auth mismatch

---

## 2. Bối cảnh dự án theo tri thức thực tế
Theo báo cáo kỹ thuật trong `export_task/`, repo hiện đang theo kiến trúc mới hơn:
- Bronze/Silver/Gold/Serving flow
- Polars là engine xử lý dữ liệu chính
- dbt được dùng cho silver/gold transform
- DuckDB / SQLMesh là phần stale / không còn dùng chính trong flow hiện tại

Điều này quyết định rằng các entry logger cũ như `duckdb_debug`, `sqlmesh_debug` không còn phù hợp và cần bị dọn sạch khỏi `shared/logger/config/logger_config.yaml`.

---

## 3. Lỗi đã fix và nguyên nhân gốc rễ

### A. Missing YAML config path
Vấn đề ban đầu:
- file config mặc định được trỏ tới đường dẫn không tồn tại:
  `platforms/orchestration/prefect/config/cophieu68_config.yaml`
- thực tế file đúng tồn tại tại:
  `flows/cophieu68_deploy_full_pipeline/ingestion/config/cophieu68_config.yaml`

Giải pháp đã áp dụng:
- cập nhật `DEFAULT_CONFIG_PATH` trong `flows/cophieu68_deploy_full_pipeline/pipeline_config.py`
- xác minh bằng command chạy thật: file tồn tại và `Loaded from .../ingestion/config/cophieu68_config.yaml`

### B. Executor lifecycle mismatch
Vấn đề tiếp theo:
- `BasePipelineOrchestrator` gọi `executor.pre_execute()` / `executor.post_execute()`
- nhưng `BronzeExecutor`, `SilverExecutor`, `GoldExecutor`, `ServingExecutor` không kế thừa từ `BaseExecutor`

Kết quả:
- Python báo `'BronzeExecutor' object has no attribute 'pre_execute'`

Giải pháp đã áp dụng:
- cho các executor kế thừa `BaseExecutor`
- cập nhật import `from flows.common.base_executor import BaseExecutor`
- pipeline đã chạy qua giai đoạn này và tiếp tục tới backend errors thực sự

---

## 4. Lỗi hiện tại thực sự đang block pipeline
Sau khi fix 2 vấn đề trên, pipeline chạy tới các stage và báo lỗi ở tầng kỹ thuật hoàn chỉnh:

### 4.1 Missing dbt project
Log thực tế:
```
Invalid value for '--project-dir': Path '/home/hungpham/DataOps_pipeline_WebCraw/dbt_project' does not exist.
```

Root cause:
- `Cophieu68PipelineConfig` default `project_dir` đang là `<repo>/dbt_project`
- folder này chưa tồn tại trên workspace hiện tại

### 4.2 Polars API mismatch
Log thực tế:
```
DataFrame.write_parquet() got an unexpected keyword argument 'storage_options'
```

tại:
- `platforms/processing/polars/polars_engine.py`

Root cause:
- wrapper hiện đang gọi `write_parquet(..., storage_options=...)`
- phiên bản Polars đang cài trong venv không tương thích với tham số này trong cách gọi hiện tại

### 4.3 PostgreSQL auth mismatch
Log thực tế:
```
FATAL: password authentication failed for user "postgres"
```

Root cause:
- ứng dụng đang cố kết nối postgres tại `localhost:5432`
- password/credential đang không khớp với service Postgres đang chạy thực tế hoặc không có service đúng như `.env`

---

## 5. File đã được điều chỉnh trong session
- `flows/cophieu68_deploy_full_pipeline/pipeline_config.py`
- `shared/logger/config/logger_config.yaml`
- `flows/cophieu68_deploy_full_pipeline/bronze.py`
- `flows/cophieu68_deploy_full_pipeline/silver.py`
- `flows/cophieu68_deploy_full_pipeline/gold.py`
- `flows/cophieu68_deploy_full_pipeline/serving.py`

---

## 6. Kết quả verify thực tế
Chạy lệnh:
```bash
cd /home/hungpham/DataOps_pipeline_WebCraw
source dataops_webcraw_env/bin/activate
python3 -m cli.main cophieu68 full --backend polars --env prod --symbols HPG
```

Hệ thống hiện chạy tới các backend layer và dừng ở các lỗi kỹ thuật thực sự thay vì các lỗi cấu hình ban đầu. Điều này chứng minh 2 nguyên nhân ban đầu đã được giải quyết:
- YAML path đã hợp lệ
- executor lifecycle đã hợp lệ

---

## 7. Nhiệm vụ ưu tiên cho ngày mai
1. Fix `dbt_project` path hoặc tạo project dbt hợp lệ
2. Sửa wrapper `PolarsEngine.write_parquet` để tương thích với phiên bản Polars đang cài
3. Kiểm tra service PostgreSQL hiện đang chạy và đồng bộ credential với `.env`
4. Re-run pipeline sau khi 3 layer này được khắc phục

---

## 8. Ghi chú lưu ý
- Không tiếp tục đổi stack hoặc thêm thư viện mới mà không xác nhận sự tương thích với môi trường hiện tại.
- Luôn ưu tiên sửa đúng theo architecture đang có trong repo: `Polars + dbt + Postgres + MinIO`.
- Mỗi issue nên được khắc phục theo thứ tự: config → executor contract → backend dependency → runtime service.

---

## 9. Hướng làm việc tiếp theo
Ngày mai bắt đầu từ đây:
- fix `dbt_project` và `PolarsEngine` trước
- sau đó kiểm tra/service Postgres và chạy lại pipeline
- nếu chạy ổn, mới xem có cần refactor thêm để clean architecture hay không

---

### Status note
Session hôm nay đã khắc phục các lỗi cấu hình ban đầu nhưng pipeline vẫn chưa chạy end-to-end vì còn 3 blocker backend rõ ràng. Đây là điểm để ngày mai tiếp tục mà không mất thời gian lặp lại quá khứ.
