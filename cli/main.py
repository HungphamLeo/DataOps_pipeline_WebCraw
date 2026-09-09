#!/usr/bin/env python3
"""
Module: cli.main
Layer: Application CLI Entry Point
Responsibility: Ultra-thin CLI entry point. Parses command line arguments, lazily resolves
                and loads pipeline orchestrators from registry, and triggers execution.
Does NOT contain: Business logic, direct configuration file loading, database or storage drivers.
"""

from __future__ import annotations

import argparse
import importlib
from pathlib import Path
import sys
from typing import Any, Dict, Tuple

# Ensure project root is available in sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


PIPELINE_REGISTRY: Dict[str, str] = {
    "cophieu68": "flows.cophieu68_deploy_full_pipeline.run",
}


def _load_orchestrator(pipeline: str, config_path: str | None) -> Tuple[Any, Any]:
    """
    Lazy dynamic loader for registered pipeline orchestrators.
    """
    if pipeline not in PIPELINE_REGISTRY:
        sys.stderr.write(f"[CLI Error] Unknown pipeline: '{pipeline}'. Registered pipelines: {', '.join(PIPELINE_REGISTRY.keys())}\n")
        sys.exit(1)

    module_path = PIPELINE_REGISTRY[pipeline]
    module = importlib.import_module(module_path)
    orchestrator = module.MasterPipelineOrchestrator(config_path=config_path)
    return orchestrator, module.format_result


def build_parser() -> argparse.ArgumentParser:
    """
    Build command line argument parser with pipeline routing and execution parameters.
    """
    parser = argparse.ArgumentParser(
        prog="cli/main.py",
        description="DataOps Pipeline CLI — Multi-tier Data Platform Execution Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "pipeline",
        choices=list(PIPELINE_REGISTRY.keys()),
        help="Target pipeline name",
    )
    parser.add_argument(
        "phase",
        choices=["bronze", "silver", "gold", "serving", "full", "validate"],
        help="Execution phase target",
    )
    parser.add_argument(
        "--symbols", "-s",
        nargs="+",
        default=None,
        metavar="SYMBOL",
        help="List of stock symbols (e.g. FPT VNM HPG)",
    )
    parser.add_argument(
        "--date", "-d",
        default=None,
        metavar="YYYY-MM-DD",
        help="Execution date (defaults to current date)",
    )
    parser.add_argument(
        "--env", "-e",
        default="prod",
        choices=["prod", "dev", "staging"],
        help="Target environment (default: prod)",
    )
    parser.add_argument(
        "--backend", "-b",
        default="polars",
        choices=["polars", "spark", "duckdb", "sqlmesh", "dbt"],
        help="Processing engine backend (default: polars)",
    )
    parser.add_argument(
        "--config", "-c",
        default=None,
        metavar="PATH",
        help="Path to YAML project config file",
    )
    parser.add_argument(
        "--output", "-o",
        default="summary",
        choices=["summary", "text", "json"],
        help="Output format (default: summary)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform dry run validation without mutating storage",
    )

    return parser


def main() -> None:
    """
    Thin entry point: parses CLI arguments and runs selected orchestrator.
    Orchestrator is responsible for resolving ExecutionPhase — CLI passes raw string.
    """
    parser = build_parser()
    args = parser.parse_args()

    orchestrator, fmt_fn = _load_orchestrator(args.pipeline, args.config)

    result = orchestrator.run(
        phase=args.phase,           # raw string — orchestrator resolves to ExecutionPhase
        symbols=args.symbols,
        backend=args.backend,
        target_date=args.date,
        environment=args.env,
        dry_run=args.dry_run,
    )

    sys.stdout.write(f"{fmt_fn(result, args.output)}\n")

    terminal_statuses = ("SUCCESS", "COMPLETED", "VALIDATION_PASSED", "SKIPPED")
    if result.get("status") not in terminal_statuses:
        sys.exit(1)


if __name__ == "__main__":
    main()
