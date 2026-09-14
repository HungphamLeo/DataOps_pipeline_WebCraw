
# Session Report: Agent Training & Skill Sync
**Date:** 2026-09-13  
**Role:** Full Stack Data Platform Engineer  
**Status:** Completed

---

## 1. Mục tiêu
Đồng bộ hóa tri thức cho agent trong repo theo đúng thứ tự ưu tiên đã quy định:
1. `./export_task/` (tri thức động, ưu tiên cao nhất)
2. `/home/hungpham/ai_workspace_management/.ai_workspace/allskill/` (tri thức chung)
3. repo context hiện tại và kiến trúc stack thực tế

Mục tiêu là đảm bảo agent có thể hoạt động đúng với repo DataOps hiện tại, không dựa vào kiến thức tổng quát sai ngữ cảnh.

---

## 2. Quy tắc đã áp dụng
- Đọc và tuân thủ file `.github/skills/copilotinstructions/SKILL.md`
- Quét `export_task/` trước khi đưa ra quyết định hoặc cập nhật cấu hình
- Đối chiếu với `allskill` để lấy chuẩn chung khi cần
- Ưu tiên kiến thức trong `export_task` khi có tương tác mâu thuẫn
- Bổ sung session summary để người dùng tiếp tục dùng cho future runs

---

## 3. Kết quả nạp vào agent
Đã cập nhật agent metadata và prompt trong:
- `.github/agents/full-stack-data-platform-engineer.agent.md`

Nội dung đã được bổ sung:
- stack thực tế hiện tại: `Polars + dbt + Postgres + MinIO + Prefect + Docker Compose`
- các lesson learned mới nhất từ session debug gần đây
- quy tắc override ưu tiên `export_task`
- các blocker hiện tại đang chờ xử lý để agent không lặp lại sai hố cũ

---

## 4. Bối cảnh thực tế repo mà agent cần nhớ
- Pipeline theo flow bronze → silver → gold → serving
- Polars là engine xử lý chính
- Prefect dùng để orchestrate flows
- dbt được dùng cho transform logic và validation
- Docker compose và runtime môi trường đã được sửa để khớp với project thực tế
- Các lỗi cũ cần tránh:
  - config path sai
  - lifecycle mismatch `pre_execute`/`post_execute`
  - dependency mismatch trong `requirements.txt`
  - Docker mount `.env` sai cách trên WSL2

---

## 5. Hướng dẫn hành vi cho agent trong tương lai
- Luôn quét `./export_task` trước khi sửa code
- Kiểm tra stack/lifecycle/config đang dùng trong repo trước khi đổi architecture
- Chỉ fix theo root cause, không thêm dependency mới nếu chưa cần thiết
- Validate bằng command thực tế và tham chiếu logs của repo
- Nếu có issue lớn, tách task theo từng layer: config → executor → backend → service runtime

---

## 6. Tài liệu tham khảo đã dùng
- `.github/skills/copilotinstructions/SKILL.md`
- `export_task/session_2026_09_12_pipeline_debugging_and_path_fix.md`
- `export_task/session_2026_09_12_docker_and_dependencies.md`
- `/home/hungpham/ai_workspace_management/.ai_workspace/allskill/Full_Stack_Data_SKILL.md`
- `/home/hungpham/ai_workspace_management/.ai_workspace/allskill/INDEX.md`

---

## 7. Gợi ý tiếp theo
- Nếu cần agent chuyên biệt khác cho một domain cụ thể (dbt, orchestration, docker, data quality), nên bổ sung thêm file agent mới trong `.github/agents/` theo cấu trúc tương tự.
- Mỗi khi có task phức tạp, nên cập nhật thêm 1 file summary mới trong `export_task/` để giữ knowledge base luôn mới.

