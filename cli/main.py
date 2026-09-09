#!/usr/bin/env python3
"""
cli/main.py — Command Line Entry Point
========================================
Trách nhiệm duy nhất: parse CLI args → gọi MasterPipelineOrchestrator.

Không chứa:
  - Business logic
  - Config loading
  - Database / storage connections

Cách dùng:
  python cli/main.py cophieu68 full   --symbols FPT VNM --date 2026-09-09
  python cli/main.py cophieu68 bronze --symbols FPT VNM
  python cli/main.py cophieu68 silver --date 2026-09-09
  python cli/main.py cophieu68 gold   --env prod
  python cli/main.py cophieu68 serving
  python cli/main.py cophieu68 validate
  python cli/main.py cophieu68 full --dry-run

Thêm pipeline mới:
  python cli/main.py <new_pipeline> full --config path/to/config.yaml
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Đảm bảo project root nằm trong sys.path để import hoạt động
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Registry: tên pipeline → module orchestrator
# ---------------------------------------------------------------------------

PIPELINE_REGISTRY = {
    "cophieu68": "flows.cophieu68_deploy_full_pipeline.run",
}


def _load_orchestrator(pipeline: str, config_path: str):
    """
    Lazy import orchestrator cho pipeline được chỉ định.
    Thêm pipeline mới: đăng ký vào PIPELINE_REGISTRY, không sửa file này.
    """
    if pipeline not in PIPELINE_REGISTRY:
        print(f"[CLI] Unknown pipeline: '{pipeline}'")
        print(f"      Available: {', '.join(PIPELINE_REGISTRY.keys())}")
        sys.exit(1)

    module_path = PIPELINE_REGISTRY[pipeline]
    import importlib
    module = importlib.import_module(module_path)
    return module.MasterPipelineOrchestrator(config_path=config_path), module.format_result


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cli/main.py",
        description="DataOps Pipeline CLI — chạy ETL pipeline theo từng phase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  python cli/main.py cophieu68 full    --symbols FPT VNM HPG
  python cli/main.py cophieu68 bronze  --symbols FPT --date 2026-09-09
  python cli/main.py cophieu68 silver  --date 2026-09-09
  python cli/main.py cophieu68 gold    --env prod
  python cli/main.py cophieu68 serving
  python cli/main.py cophieu68 validate --dry-run
        """,
    )

    parser.add_argument(
        "pipeline",
        choices=list(PIPELINE_REGISTRY.keys()),
        help="Tên pipeline cần chạy",
    )
    parser.add_argument(
        "phase",
        choices=["bronze", "silver", "gold", "serving", "full", "validate"],
        help="Phase cần thực thi",
    )
    parser.add_argument(
        "--symbols", "-s",
        nargs="+",
        default=None,
        metavar="SYMBOL",
        help="Danh sách mã cổ phiếu (vd: FPT VNM HPG). Mặc định: FPT VNM HPG MBB SSI",
    )
    parser.add_argument(
        "--date", "-d",
        default=None,
        metavar="YYYY-MM-DD",
        help="Ngày xử lý (mặc định: hôm nay)",
    )
    parser.add_argument(
        "--env", "-e",
        default="prod",
        choices=["prod", "dev", "staging"],
        help="Environment cho SQLMesh models (mặc định: prod)",
    )
    parser.add_argument(
        "--backend", "-b",
        default="polars",
        choices=["polars", "spark", "duckdb", "sqlmesh", "dbt"],
        help="Processing backend (mặc định: polars)",
    )
    parser.add_argument(
        "--config", "-c",
        default=None,
        metavar="PATH",
        help="Đường dẫn tới config YAML (mặc định: platforms/orchestration/prefect/config/cophieu68_config.yaml)",
    )
    parser.add_argument(
        "--output", "-o",
        default="summary",
        choices=["summary", "text", "json"],
        help="Định dạng output (mặc định: summary)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config, không thực thi pipeline",
    )

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    # Load orchestrator cho pipeline được chọn
    orchestrator, fmt_fn = _load_orchestrator(args.pipeline, args.config)

    from flows.cophieu68_deploy_full_pipeline.context import ExecutionPhase

    # Chạy pipeline
    result = orchestrator.run(
        phase=ExecutionPhase(args.phase),
        symbols=args.symbols,
        backend=args.backend,
        target_date=args.date,
        environment=args.env,
        dry_run=args.dry_run,
    )

    # In kết quả
    print(fmt_fn(result, args.output))

    # Exit code: 1 nếu thất bại
    terminal_statuses = ("SUCCESS", "COMPLETED", "VALIDATION_PASSED", "SKIPPED")
    if result.get("status") not in terminal_statuses:
        sys.exit(1)


if __name__ == "__main__":
    main()
