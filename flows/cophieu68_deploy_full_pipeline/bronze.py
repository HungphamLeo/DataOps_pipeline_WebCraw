"""
bronze.py — Bronze Phase Module
================================
Ingest raw data từ crawlers → MinIO Bronze Parquet.

Luồng:
  ExtractCophieu68.crawl_*()
    → BronzeIngester.ingest() / .process()
    → PolarsEngine.write_parquet() → s3://lakehouse/bronze/<table>/

Dùng:
  - platforms/processing/polars/    → write Parquet to S3/MinIO
  - platforms/processing/base_processing_subsystem/ → DQ, error logging
  - flows/.../schema/schema_registry → TableDef + Polars schema enforcement
  - flows/.../ingestion/             → ExtractCophieu68

Không chứa:
  - Transform logic (silver.py)
  - PostgreSQL sync (serving.py)
  - dbt / SQL models (silver/gold)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from platforms.processing.base_processing_subsystem import (
    CleansingRuleSet,
    ErrorLevel,
)
from platforms.factory.client_factory import build_polars_engine

from flows.shared.context import ExecutionContext, make_batch_id
from flows.cophieu68_deploy_full_pipeline.pipeline_config import Cophieu68PipelineConfig
from flows.cophieu68_deploy_full_pipeline.builders import (
    build_extractor,
    build_cleansing_rules,
)
from flows.cophieu68_deploy_full_pipeline.schema import (
    get_bronze_table,
    ALL_BRONZE_TABLES,
)


# ─────────────────────────────────────────────────────────────────────────────
# BronzeIngester — flatten + DQ + write Parquet
# ─────────────────────────────────────────────────────────────────────────────

class BronzeIngester:
    """
    Nhận raw records từ crawler, áp dụng DQ cleansing,
    ghi Parquet phân vùng theo ingest_date lên MinIO bronze/.
    """

    def __init__(
        self,
        engine,
        base_path: str = "s3://lakehouse",
        governance_logger: Optional[logging.Logger] = None,
    ) -> None:
        self.engine           = engine
        self.base_path        = base_path
        self.logger           = engine.logger
        self.governance_logger = governance_logger or engine.logger

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _profile(records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not records:
            return {"total_rows": 0, "columns": []}
        cols = list(records[0].keys())
        null_counts = {c: sum(1 for r in records if r.get(c) is None) for c in cols}
        return {"total_rows": len(records), "columns": cols, "null_counts": null_counts}

    def _apply_dq(
        self,
        records: List[Dict[str, Any]],
        cleansing: CleansingRuleSet,
        run_id: str,
    ) -> tuple:
        """Stamp _dq_status onto each record; return (enriched, violations)."""
        processed, violations = [], []
        for rec in records:
            valid, messages = cleansing.apply(rec)
            rec["_dq_status"] = "PASS" if valid else "WARN"
            rec["_dq_errors"] = "; ".join(messages) if messages else ""
            rec["_run_id"]    = run_id
            if not valid:
                violations.append({"record": rec, "messages": messages})
            processed.append(rec)
        return processed, violations

    @staticmethod
    def _flatten(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Cast every value to str (Bronze = raw strings only).
        Handles nested DataFrames, numpy scalars, arrays.
        """
        import pandas as _pd
        import numpy as _np

        flat = []
        for rec in records:
            out: Dict[str, Any] = {}
            for k, v in rec.items():
                if not isinstance(k, str):
                    continue
                if isinstance(v, _pd.DataFrame):
                    out[k] = v.to_json(orient="records")
                elif v is None:
                    out[k] = None
                elif isinstance(v, (_np.integer, _np.floating)):
                    out[k] = str(v.item())
                elif isinstance(v, _np.ndarray):
                    out[k] = str(v.tolist())
                else:
                    out[k] = str(v)
            flat.append(out)
        return flat

    def _audit_cols(self, df: Any, batch_id: str, run_id: str) -> Any:
        """Attach _batch_id, _run_id, _ingest_timestamp, ingest_date."""
        import polars as pl
        now = datetime.now(timezone.utc)
        return df.with_columns([
            pl.lit(batch_id).alias("_batch_id"),
            pl.lit(run_id).alias("_run_id"),
            pl.lit(now.isoformat()).alias("_ingest_timestamp"),
            pl.lit(now.date().isoformat()).alias("ingest_date"),
        ])

    # ------------------------------------------------------------------
    # process — trading data with explicit DQ gate + reject path
    # ------------------------------------------------------------------

    def process(
        self,
        raw_records: List[Dict[str, Any]],
        batch_id: str,
        run_id: str,
        symbol: str,
        cleansing: Optional[CleansingRuleSet] = None,
    ) -> Dict[str, Any]:
        import polars as pl

        profile = self._profile(raw_records)

        if cleansing:
            enriched, violations = self._apply_dq(raw_records, cleansing, run_id)
            if violations:
                self.logger.warning("[Bronze:%s] %d DQ violations", symbol, len(violations))
                for idx, v in enumerate(violations, 1):
                    self.governance_logger.warning(
                        "[DQ][%s] %d/%d: %s",
                        symbol, idx, len(violations), "; ".join(v["messages"]),
                    )
        else:
            enriched, violations = raw_records, []

        if not enriched:
            return {
                "saved_path": None, "reject_path": None,
                "stats": {**profile, "clean": 0, "dq_violations": len(violations)},
            }

        df = pl.DataFrame(enriched)
        if "symbol" not in df.columns:
            df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))
        df = self._audit_cols(df, batch_id, run_id)

        path = f"{self.base_path}/bronze/stock_prices/"
        self.engine.write_parquet(df=df, target_path=path, partition_by=["ingest_date"])

        reject_path = None
        if violations:
            reject_path = f"{self.base_path}/bronze/_dq_violations/stock_prices/"
            reject_df = pl.DataFrame([v["record"] for v in violations])
            self.engine.write_parquet(df=reject_df, target_path=reject_path)

        return {
            "saved_path": path, "reject_path": reject_path,
            "stats": {**profile, "clean": len(enriched), "dq_violations": len(violations)},
        }

    # ------------------------------------------------------------------
    # ingest — generic, all tables
    # ------------------------------------------------------------------

    def ingest(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        batch_id: str,
        run_id: str,
        symbol: Optional[str] = None,
        cleansing: Optional[CleansingRuleSet] = None,
    ) -> Dict[str, Any]:
        """
        Generic ingest — flatten → DQ → enforce schema từ schema_registry → write Parquet.

        Schema enforcement:
          - Nếu table_name có trong ALL_BRONZE_TABLES → dùng TableDef.to_polars_schema()
            (all Utf8 cho bronze layer, tự động bổ sung audit cols)
          - Fallback: all-Utf8 schema từ keys của record đầu tiên
        """
        import polars as pl

        if not records:
            self.logger.warning("[Bronze] No records for table=%s symbol=%s", table_name, symbol)
            return {"saved_path": None, "rows": 0, "dq_violations": 0}

        flat = self._flatten(records)

        violations: List = []
        if cleansing:
            flat, violations = self._apply_dq(flat, cleansing, run_id)
        if not flat:
            return {"saved_path": None, "rows": 0, "dq_violations": 0}

        # ── Schema enforcement từ registry ──────────────────────────────
        tbl_def = get_bronze_table(table_name)
        if tbl_def is not None:
            registry_schema = tbl_def.to_polars_schema()
            # Chỉ enforce các cột tồn tại trong data — tránh crash khi crawler
            # trả về ít cột hơn schema (Bronze = additive, không strict)
            str_schema = {
                col: dtype
                for col, dtype in registry_schema.items()
                if col in flat[0]
            }
        else:
            # Bảng chưa khai báo trong registry — all-Utf8 fallback
            self.logger.debug("[Bronze] table=%s not in schema_registry — using all-Utf8", table_name)
            str_schema = {c: pl.Utf8 for c in flat[0] if isinstance(c, str)}

        df = pl.DataFrame(flat, schema_overrides=str_schema)

        if symbol and "symbol" not in df.columns:
            df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))
        df = self._audit_cols(df, batch_id, run_id)

        # Partition_by từ registry nếu có, fallback về ingest_date
        partition_cols = tbl_def.partition_by if tbl_def else ["ingest_date"]
        # Đảm bảo ingest_date luôn có (đã được thêm bởi _audit_cols)
        partition_cols = [c for c in partition_cols if c in df.columns] or ["ingest_date"]

        path = f"{self.base_path}/bronze/{table_name}/"
        self.engine.write_parquet(df=df, target_path=path, partition_by=partition_cols)
        self.logger.info("[Bronze] Wrote %d rows → %s (schema=%s)", len(df), path,
                         "registry" if tbl_def else "fallback")

        return {"saved_path": path, "rows": len(df), "dq_violations": len(violations)}


# ─────────────────────────────────────────────────────────────────────────────
# BronzeExecutor — orchestrate all crawl → ingest steps
# ─────────────────────────────────────────────────────────────────────────────

class BronzeExecutor:
    """
    Orchestrates full Bronze ingestion: crawl all cophieu68 endpoints
    → flatten → DQ → write Parquet to MinIO.
    """

    def __init__(
        self,
        context: ExecutionContext,
        config: "Cophieu68PipelineConfig",
    ) -> None:
        self.context = context
        self.config  = config
        self.logger  = context.logger

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _lineage(self, source: str, table: str, rows: int) -> None:
        self.context.metadata_repo.log_lineage(
            run_id=self.context.run_id,
            source_layer="external", source_table=source,
            target_layer="bronze",   target_table=table,
            operation="APPEND",      rows_affected=rows,
        )

    def _dq_warn(self, table: str, count: int) -> None:
        if count > 0:
            self.context.error_log.add(
                ErrorLevel.WARNING, f"{count} DQ issues in bronze.{table}"
            )

    # ------------------------------------------------------------------
    # Per-symbol steps
    # ------------------------------------------------------------------

    def _step_trading(self, ext, ing, sym, bid, res) -> None:
        data = ext.crawl_trading_data(symbol=sym, page=1)
        if not data or not data.get("records"):
            self.logger.warning("[Bronze] No trading records for %s", sym)
            return
        r = ing.process(
            raw_records=data["records"], batch_id=bid,
            run_id=self.context.run_id, symbol=sym,
            cleansing=build_cleansing_rules(sym),
        )
        rows = r["stats"]["clean"]
        res["rows_ingested"] += rows
        res["dq_issues"]     += r["stats"].get("dq_violations", 0)
        self._dq_warn("stock_prices", r["stats"].get("dq_violations", 0))
        self._lineage(f"cophieu68_{sym}", "bronze_stock_prices", rows)

    def _step_company_profile(self, ext, ing, sym, bid, res) -> None:
        raw = ext.crawl_company_profile(symbol=sym)
        if not raw:
            return
        rec = raw if isinstance(raw, dict) else getattr(raw, "__dict__", None)
        if not rec:
            return
        r = ing.ingest("company_profile", [rec], bid, self.context.run_id, sym)
        res["rows_ingested"] += r["rows"]
        self._lineage(f"cophieu68_{sym}", "bronze_company_profile", r["rows"])

    def _step_financial_ratios(self, ext, ing, sym, bid, res) -> None:
        raw = ext.crawl_financial_ratios(symbol=sym)
        if not raw:
            return
        rec = raw if isinstance(raw, dict) else getattr(raw, "__dict__", None)
        if not rec:
            return
        r = ing.ingest("financial_ratios", [rec], bid, self.context.run_id, sym)
        res["rows_ingested"] += r["rows"]
        self._lineage(f"cophieu68_{sym}", "bronze_financial_ratios", r["rows"])

    def _step_financial_report_summary(self, ext, ing, sym, bid, res) -> None:
        raw = ext.crawl_financial_report_summary(symbol=sym)
        if not raw:
            return
        records: List[Dict[str, Any]] = []
        for report_key, report_obj in raw.items():
            df = getattr(report_obj, "data", None)
            if df is None:
                continue
            try:
                for row in df.to_dict(orient="records"):
                    row["symbol"]      = sym.upper()
                    row["report_type"] = report_key
                    records.append(row)
            except Exception:
                continue
        if not records:
            return
        r = ing.ingest("financial_report_summary", records, bid, self.context.run_id, sym)
        res["rows_ingested"] += r["rows"]
        self._lineage(f"cophieu68_{sym}", "bronze_financial_report_summary", r["rows"])

    def _step_business_plan(self, ext, ing, sym, bid, res) -> None:
        raw = ext.crawl_business_plan(symbol=sym)
        if not raw or not raw.get("data"):
            return
        r = ing.ingest("business_plan", raw["data"], bid, self.context.run_id, sym)
        res["rows_ingested"] += r["rows"]
        self._lineage(f"cophieu68_{sym}", "bronze_business_plan", r["rows"])

    def _step_income_statement(self, ext, ing, sym, bid, res, report_type: str) -> None:
        raw = ext.crawl_details_income_statement(symbol=sym, report_type=report_type)
        if not raw:
            return
        df = raw.get("data") if isinstance(raw, dict) else getattr(raw, "data", None)
        if df is None:
            return
        try:
            records = df.to_dict(orient="records")
            for row in records:
                row["symbol"]      = sym.upper()
                row["report_type"] = report_type
        except Exception:
            return
        table = f"income_statement_{report_type}"
        r = ing.ingest(table, records, bid, self.context.run_id, sym)
        res["rows_ingested"] += r["rows"]
        self._lineage(f"cophieu68_{sym}", f"bronze_{table}", r["rows"])

    def _step_balance_sheet(self, ext, ing, sym, bid, res, report_type: str) -> None:
        raw = ext.crawl_details_balance_sheet(symbol=sym, report_type=report_type)
        if not raw:
            return
        df = raw.get("data") if isinstance(raw, dict) else getattr(raw, "data", None)
        if df is None:
            return
        try:
            records = df.to_dict(orient="records")
            for row in records:
                row["symbol"]      = sym.upper()
                row["report_type"] = report_type
        except Exception:
            return
        table = f"balance_sheet_{report_type}"
        r = ing.ingest(table, records, bid, self.context.run_id, sym)
        res["rows_ingested"] += r["rows"]
        self._lineage(f"cophieu68_{sym}", f"bronze_{table}", r["rows"])

    # ------------------------------------------------------------------
    # Global steps (run once, not per-symbol)
    # ------------------------------------------------------------------

    def _step_industry_sectors(self, ext, ing, bid, res) -> None:
        rows = ext.crawl_company_info_belong_to_industry_sectors()
        if not rows:
            return
        records = [r if isinstance(r, dict) else r.__dict__ for r in rows]
        r = ing.ingest("industry_sectors", records, bid, self.context.run_id)
        res["rows_ingested"] += r["rows"]
        self._lineage("cophieu68_global", "bronze_industry_sectors", r["rows"])

    def _step_market_type_sectors(self, ext, ing, bid, res) -> None:
        rows = ext.crawl_company_info_belong_to_market_type()
        if not rows:
            return
        records = [r if isinstance(r, dict) else r.__dict__ for r in rows]
        r = ing.ingest("market_type_sectors", records, bid, self.context.run_id)
        res["rows_ingested"] += r["rows"]
        self._lineage("cophieu68_global", "bronze_market_type_sectors", r["rows"])

    def _step_industry_info(self, ext, ing, bid, res, type_info: str) -> None:
        raw = ext.crawl_industry_info(type_info=type_info)
        if not raw:
            return
        records = []
        for key, row in raw.items():
            rec = row if isinstance(row, dict) else row.__dict__
            rec["_industry_key"] = key
            records.append(rec)
        if not records:
            return
        table = f"industry_info_{type_info}"
        r = ing.ingest(table, records, bid, self.context.run_id)
        res["rows_ingested"] += r["rows"]
        self._lineage("cophieu68_global", f"bronze_{table}", r["rows"])

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    def execute(self) -> Dict[str, Any]:
        from shared.logger.python_main_logger import logger_manager

        self.logger.info(
            "[BronzeExecutor] Starting bronze phase symbols=%s", self.context.symbols
        )
        result: Dict[str, Any] = {
            "phase": "bronze",
            "symbols_processed": 0,
            "rows_ingested": 0,
            "dq_issues": 0,
            "errors": 0,
        }

        engine   = build_polars_engine(**self.config.polars_build_params)
        gov_log  = logger_manager.get_logger("logger.governance.data_quality")
        ingester = BronzeIngester(
            engine=engine,
            base_path=self.config.lakehouse_base,
            governance_logger=gov_log,
        )
        extractor    = build_extractor(self.config)
        global_batch = make_batch_id("GLOBAL")

        # ── Per-symbol ────────────────────────────────────────────────
        for sym in self.context.symbols:
            bid = make_batch_id(sym)
            self.logger.info("[BronzeExecutor] symbol=%s batch=%s", sym, bid)
            ok = True

            per_symbol_steps = [
                ("trading_data",             lambda: self._step_trading(extractor, ingester, sym, bid, result)),
                ("company_profile",          lambda: self._step_company_profile(extractor, ingester, sym, bid, result)),
                ("financial_ratios",         lambda: self._step_financial_ratios(extractor, ingester, sym, bid, result)),
                ("financial_report_summary", lambda: self._step_financial_report_summary(extractor, ingester, sym, bid, result)),
                ("business_plan",            lambda: self._step_business_plan(extractor, ingester, sym, bid, result)),
                ("income_statement_quarter", lambda: self._step_income_statement(extractor, ingester, sym, bid, result, "quarter")),
                ("income_statement_year",    lambda: self._step_income_statement(extractor, ingester, sym, bid, result, "year")),
                ("balance_sheet_quarter",    lambda: self._step_balance_sheet(extractor, ingester, sym, bid, result, "quarter")),
                ("balance_sheet_year",       lambda: self._step_balance_sheet(extractor, ingester, sym, bid, result, "year")),
            ]
            for step_name, fn in per_symbol_steps:
                try:
                    fn()
                except Exception as exc:
                    self.logger.error("[Bronze] %s/%s failed: %s", sym, step_name, exc)
                    self.context.error_log.add(
                        ErrorLevel.WARNING, f"Bronze {sym}/{step_name}: {exc}"
                    )
                    ok = False

            result["symbols_processed"] += 1
            if not ok:
                result["errors"] += 1

        # ── Global (run once) ─────────────────────────────────────────
        try:
            self._step_industry_sectors(extractor, ingester, global_batch, result)
            self._step_market_type_sectors(extractor, ingester, global_batch, result)
            for kind in ("summary_info", "financial_info", "fund_info"):
                self._step_industry_info(extractor, ingester, global_batch, result, kind)
        except Exception as exc:
            self.logger.error("[Bronze] Global crawlers failed: %s", exc)
            self.context.error_log.add(ErrorLevel.ERROR, f"Bronze global crawl failed: {exc}")
            result["errors"] += 1

        self.logger.info(
            "[BronzeExecutor] Done: rows=%d symbols=%d errors=%d",
            result["rows_ingested"], result["symbols_processed"], result["errors"],
        )
        return result
