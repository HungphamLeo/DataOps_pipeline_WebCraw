
# Session Report: Dockerization & Dependency Resolution
**Date:** 2026-09-12  
**Role:** Full Stack Data Platform Engineer  
**Status:** Completed & Optimized

---

## 1. Tóm tắt Vấn đề & Nguyên nhân Gốc rễ

### A. Lỗi Mismatch Cấu trúc Dự án trong Dockerfile cũ
- **Hiện tượng:** Dockerfile cũ thực hiện sao chép các tệp `requirements_common.txt`, `requirements_prefect.txt` và các thư mục `services/`, `tests/` vốn **không tồn tại** trong cấu trúc dự án thực tế.
- **Nguyên nhân:** Cấu hình Docker cũ được viết cho một dự án/nhánh khác, không khớp với dự án DataOps hiện tại (chỉ có duy nhất `requirements.txt` ở thư mục gốc).

### B. Lỗi OCI Runtime Mount `.env` (WSL2/Docker Desktop)
- **Hiện tượng:** Lỗi `runc create failed: ... not a directory: Are you trying to mount a directory onto a file (or vice-versa)?` khi chạy `docker compose up`.
- **Nguyên nhân:** Trên hệ thống WSL2, khi bind mount trực tiếp một tệp văn bản chưa tồn tại hoặc bị lệch đường dẫn như `- ../.env:/app/.env:ro`, Docker Daemon tự động tạo một **thư mục trống** `.env` trên máy host. Khi mount một thư mục đè lên một vị trí file trong container, OCI runtime báo lỗi nghiêm trọng.

### C. Lỗi Trùng Lặp & Xung Đột Dependency (ResolutionImpossible)
- **Hiện tượng:** Lỗi build dừng lại ở bước cài đặt `requirements.txt`:
  - `No matching distribution found for pendulum==2.3.1` (phiên bản không tồn tại trên PyPI).
  - Conflict giữa `pytz==2025.2`, `croniter` và `prefect 2.14.0` (Prefect yêu cầu `pytz<2024` và `pendulum<3`).

---

## 2. Các Giải pháp Đã Triển khai (Actions Taken)

### A. Tối ưu hóa & Làm sạch `requirements.txt`
- Hạ cấp và nới lỏng đúng quy chuẩn các thư viện lõi để tương thích với **Prefect 2.14**:
  - `pendulum>=2.1.2,<3.0` (Khắc phục lỗi không có phiên bản `2.3.1`).
  - `pytz>=2021.1,<2024` (Giải quyết triệt để xung đột phiên bản với `croniter` và `pandas`).
  - Thêm tường minh `polars==1.12.0` đáp ứng engine xử lý dữ liệu được thiết kế sẵn.

### B. Tái cấu trúc `infra/Dockerfile`
- Viết lại Dockerfile tối giản, an toàn và sạch sẽ:
  - Sử dụng image cơ sở `python:3.11-slim`.
  - Copy đúng các module thư mục hiện có: `cli/`, `flows/`, `platforms/`, `shared/`, `scripts/`, `sql/`, `logs/`.
  - Cài đặt dependencies tập trung từ `requirements.txt` chuẩn của repo.

### C. Đồng bộ hóa `infra/docker_compose.yml`
- Sửa lại toàn bộ đường dẫn `context` từ `../..` thành `..` (Do compose nằm trong thư mục con `infra/`).
- Khắc phục lỗi OCI mount bằng cách đổi hoàn toàn cơ chế mount file `.env` thành sử dụng cấu hình **`env_file: - ../.env`**.
- Định nghĩa rõ ràng các service: `prefect-server`, `prefect-worker`, `postgres-local`, `minio`, `spark-master`, `spark-worker` và thêm service `pipeline-runner` dùng chung.

### D. Khởi tạo Môi trường ảo (Virtual Environment)
- Thực hiện thiết lập môi trường biệt lập `dataops_webcraw_env` tại thư mục gốc để nhà phát triển có thể debug trực tiếp ngoài Docker:
  ```bash
  python3 -m venv dataops_webcraw_env
  source dataops_webcraw_env/bin/activate
  pip install -r requirements.txt
  ```

---

## 3. Hướng dẫn Lệnh Thực thi Hệ thống

### A. Vận hành Stack Cơ sở hạ tầng (Docker Compose)
Để chạy các dịch vụ nền tảng (MinIO, Postgres, Prefect, Spark):
```bash
cd /home/hungpham/DataOps_pipeline_WebCraw/infra

# 1. Dọn dẹp tài nguyên cũ bị lỗi (nếu có)
docker compose -f docker_compose.yml down --volumes --remove-orphans

# 2. Khởi động và build lại toàn bộ Container
docker compose -f docker_compose.yml up --build -d

# 3. Kiểm tra trạng thái các service
docker compose -f docker_compose.yml ps
```

### B. Thực thi Pipeline Xử lý Dữ liệu

#### Cách 1: Thực thi trong Docker container (Khuyên dùng)
```bash
# Chạy Full pipeline (từ Bronze -> Serving)
docker compose -f docker_compose.yml exec pipeline-runner python -m cli.main cophieu68 full --backend polars --env prod

# Chạy thử nghiệm chỉ định mã cổ phiếu
docker compose -f docker_compose.yml exec pipeline-runner python -m cli.main cophieu68 full --backend polars --env prod --symbols FPT HPG VNM
```

#### Cách 2: Thực thi trực tiếp trên máy host (WSL2)
```bash
cd /home/hungpham/DataOps_pipeline_WebCraw
source dataops_webcraw_env/bin/activate

# Chạy Full pipeline bằng Python local
python -m cli.main cophieu68 full --backend polars --env prod
```

---

## 4. Bài học Kinh nghiệm & Khuyến nghị Lâu dài
1. **WSL2 Volume Mount:** Hạn chế mount trực tiếp các file cấu hình đơn lẻ chưa khởi tạo sẵn từ WSL2 vào Container. Giải pháp dùng thuộc tính `env_file` của Compose luôn an toàn và ít lỗi runtime hơn.
2. **Version Lock:** Luôn kiểm tra tính tương thích ngược của các thư viện bổ trợ khi dùng các Orchestrator lớn như Prefect (ví dụ: Prefect 2.x có giới hạn rất chặt chẽ về phiên bản của `pendulum`, `pytz` và `marshmallow`).
3. **Compose Pathing:** Đặt `context: ..` trong compose nằm ở thư mục con để định vị đúng root dự án khi build image.

