"""
Bronze Phase Module
===================
Chứa BronzePolarsIngester và BronzeExecutor — tách khỏi monolith.

SRP: chỉ xử lý ingestion raw data → MinIO Bronze Parquet.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from flows.cophieu68_deploy_full_pipeline.context import (
    ExecutionContext,
    LAKEHOUSE_BASE,
    make_batch_id,
)
from flows.cophieu68_deploy_full_pipeline.builders import (
    build_polars_engine,
    build_extractor,
    build_cleansing_rules,
    build_dq_ruleset,
)


class BronzePolarsIngester:
    """Normalise raw records and write Parquet to MinIO bronze layer."""

    def __init__(
        self,
        engine,
        table_name: str = "stock_prices",
        base_path: str = LAKEHOUSE_BASE,
        governance_logger: Optional[logging.Logger] = None,
    ) -> None:
        self.engine = engine
        self.table_name = table_name
        self.base_path = base_path
        self.logger = engine.logger
        self.governance_logger = governance_logger or engine.logger

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _profile(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not records:
            return {"total_rows": 0, "columns": []}
        cols = list(records[0].keys())
        total = len(records)
        null_counts = {c: sum(1 for r in records if r.get(c) is None) for c in cols}
        return {"total_rows": total, "columns": cols, "null_counts": null_counts}

    def _evaluate_dq(self, records, cleansing, run_id):
        import polars as pl
        processed, violations = [], []
        for rec in records:
            valid, messages = cleansing.apply(rec)
            rec["_dq_status"] = "PASS" if valid else "WARN"
            rec["_dq_errors"] = "; ".join(messages) if messages else ""
            rec["_run_id"] = run_id
            if not valid:
                violations.append({"record": rec, "messages": messages})
            processed.append(rec)
        return processed, violations

    def _flatten_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Cast every value to str (Bronze = raw), flatten nested DataFrames."""
        import pandas as _pd
        import numpy as _np
        flat = []
        for rec in records:
            flat_rec: Dict[str, Any] = {}
            for k, v in rec.items():
                if not isinstance(k, str):
                    continue
                if isinstance(v, _pd.DataFrame):
                    flat_rec[k] = v.to_json(orient="records")
                elif v is None:
                    flat_rec[k] = None
                elif isinstance(v, (_np.integer, _np.floating)):
                    flat_rec[k] = str(v.item())
                elif isinstance(v, _np.ndarray):
                    flat_rec[k] = str(v.tolist())
                else:
                    flat_rec[k] = str(v)
            flat.append(flat_rec)
        return flat

    # ------------------------------------------------------------------
    # Public: process (trading data with DQ gate)
    # ------------------------------------------------------------------

    def process(
        self,
        raw_records: List[Dict[str, Any]],
        batch_id: str,
        run_id: str,
        symbol: str,
        cleansing=None,
    ) -> Dict[str, Any]:
        import polars as pl

        profile = self._profile(raw_records)
        if cleansing:
            enriched, violations = self._evaluate_dq(raw_records, cleansing, run_id)
        else:
            enriched, violations = raw_records, []

        if violations:
            self.logger.warning(
                "[Bronze:%s] %d DQ violations found", symbol, len(violations)
            )
            for idx, v in enumerate(violations, 1):
                self.governance_logger.warning(
                    "[DQ][%s] %d/%d: %s",
                    symbol, idx, len(violations),
                    "; ".join(v["messages"]),
                )

        if not enriched:
            return {
                "saved_path": None,
                "reject_path": None,
                "stats": {**profile, "clean": 0, "dq_violations": len(violations)},
            }

        df = pl.DataFrame(enriched)
        if "symbol" not in df.columns:
            df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))

        df = df.with_columns([
            pl.lit(batch_id).alias("_batch_id"),
            pl.lit(run_id).alias("_run_id"),
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingest_timestamp"),
            pl.lit(datetime.now(timezone.utc).date().isoformat()).alias("ingest_date"),
        ])

        bronze_path = f"{self.base_path}/bronze/{self.table_name}/"
        saved_path = self.engine.write_parquet(
            df=df, target_path=bronze_path, partition_by=["ingest_date"]
        )

        reject_path = None
        if violations:
            reject_path = f"{self.base_path}/bronze/_dq_violations/{self.table_name}/"
            reject_df = pl.DataFrame([v["record"] for v in violations])
            self.engine.write_parquet(df=reject_df, target_path=reject_path)

        return {
            "saved_path": saved_path,
            "reject_path": reject_path,
            "stats": {**profile, "clean": len(enriched), "dq_violations": len(violations)},
        }

    # ------------------------------------------------------------------
    # Public: ingest (generic, all tables)
    # ------------------------------------------------------------------

    def ingest(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        batch_id: str,
        run_id: str,
        symbol: Optional[str] = None,
        cleansing=None,
    ) -> Dict[str, Any]:
        import polars as pl

        if not records:
            self.logger.warning(
                "[Bronze] No records for table=%s symbol=%s", table_name, symbol
            )
            return {"saved_path": None, "rows": 0, "dq_violations": 0}

        flat = self._flatten_records(records)

        if cleansing:
            flat, violations = self._evaluate_dq(flat, cleansing, run_id)
        else:
            violations = []

        if not flat:
            return {"saved_path": None, "rows": 0, "dq_violations": 0}

        str_cols = {c: pl.Utf8 for c in flat[0].keys() if isinstance(c, str)}
        df = pl.DataFrame(flat, schema_overrides=str_cols)

        if symbol and "symbol" not in df.columns:
            df = df.with_columns(pl.lit(symbol.upper()).alias("symbol"))

        df = df.with_columns([
            pl.lit(batch_id).alias("_batch_id"),
            pl.lit(run_id).alias("_run_id"),
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingest_timestamp"),
            pl.lit(datetime.now(timezone.utc).date().isoformat()).alias("ingest_date"),
        ])

        path = f"{self.base_path}/bronze/{table_name}/"
        self.engine.write_parquet(df=df, target_path=path, partition_by=["ingest_date"])
        self.logger.info("[Bronze] Wrote %d rows → %s", len(df), path)

        return {"saved_path": path, "rows": len(df), "dq_violations": len(violations)}


class BronzeExecutor:
    """Orchestrates full Bronze ingestion for all crawl_* methods."""

    def __init__(self, context: ExecutionContext, config: Dict[str, Any]) -> None:
        self.context = context
        self.config = config
        self.logger = context.logger

    # ------------------------------------------------------------------
    # Lineage + DQ helpers
    # ------------------------------------------------------------------

    def _log_lineage(self, source: str, table: str, rows: int) -> None:
        self.context.metadata_repo.log_lineage(
            run_id=self.context.run_id,
            source_layer="external",
            source_table=source,
            target_layer="bronze",
            target_table=table,
            operation="APPEND",
            rows_affected=rows,
        )

    def _warn_dq(self, table: str, count: int) -> None:
        if count > 0:
            from platforms.processing.base_processing_subsystem import ErrorLevel
            self.context.error_log.add(
                ErrorLevel.WARNING,
                f"{count} DQ issues in bronze.{table}",
            )

    # ------------------------------------------------------------------
    # Per-symbol crawlers
    # ------------------------------------------------------------------

    def _ingest_trading_data(self, extractor, ingester, symbol, batch_id, result):
        data = extractor.crawl_trading_data(symbol=symbol, page=1)
        if not data or not data.get("records"):
            self.logger.warning("[Bronze] No trading records for %s", symbol)
            return
        res = ingester.process(
            raw_records=data["records"],
            batch_id=batch_id,
            run_id=self.context.run_id,
            symbol=symbol,
            cleansing=build_cleansing_rules(symbol),
        )
        rows = res["stats"]["clean"]
        dq   = res["stats"].get("dq_violations", 0)
        result["rows_ingested"] += rows
        result["dq_issues"]     += dq
        self._warn_dq("stock_prices", dq)
        self._log_lineage(f"cophieu68_{symbol}", "bronze_stock_prices", rows)

    def _ingest_company_profile(self, extractor, ingester, symbol, batch_id, result):
        raw = extractor.crawl_company_profile(symbol=symbol)
        if not raw:
            return
        rec = raw if isinstance(raw, dict) else (raw.__dict__ if hasattr(raw, "__dict__") else None)
        if not rec:
            return
        res = ingester.ingest("company_profile", [rec], batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", "bronze_company_profile", res["rows"])

    def _ingest_financial_ratios(self, extractor, ingester, symbol, batch_id, result):
        raw = extractor.crawl_financial_ratios(symbol=symbol)
        if not raw:
            return
        rec = raw if isinstance(raw, dict) else (raw.__dict__ if hasattr(raw, "__dict__") else None)
        if not rec:
            return
        res = ingester.ingest("financial_ratios", [rec], batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", "bronze_financial_ratios", res["rows"])

    def _ingest_financial_report_summary(self, extractor, ingester, symbol, batch_id, result):
        raw = extractor.crawl_financial_report_summary(symbol=symbol)
        if not raw:
            return
        records: List[Dict[str, Any]] = []
        for report_key, report_obj in raw.items():
            df = getattr(report_obj, "data", None)
            if df is None:
                continue
            try:
                sub = df.to_dict(orient="records")
                for r in sub:
                    r["symbol"] = symbol.upper()
                    r["report_type"] = report_key
                    records.append(r)
            except Exception:
                continue
        if not records:
            return
        res = ingester.ingest("financial_report_summary", records, batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", "bronze_financial_report_summary", res["rows"])

    def _ingest_business_plan(self, extractor, ingester, symbol, batch_id, result):
        raw = extractor.crawl_business_plan(symbol=symbol)
        if not raw or not raw.get("data"):
            return
        res = ingester.ingest("business_plan", raw["data"], batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", "bronze_business_plan", res["rows"])

    def _ingest_income_statement(self, extractor, ingester, symbol, batch_id, result, report_type):
        raw = extractor.crawl_details_income_statement(symbol=symbol, report_type=report_type)
        if not raw:
            return
        df = raw.get("data") if isinstance(raw, dict) else getattr(raw, "data", None)
        if df is None:
            return
        try:
            records = df.to_dict(orient="records")
            for r in records:
                r["symbol"] = symbol.upper()
                r["report_type"] = report_type
        except Exception:
            return
        table = f"income_statement_{report_type}"
        res = ingester.ingest(table, records, batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", f"bronze_{table}", res["rows"])

    def _ingest_balance_sheet(self, extractor, ingester, symbol, batch_id, result, report_type):
        raw = extractor.crawl_details_balance_sheet(symbol=symbol, report_type=report_type)
        if not raw:
            return
        df = raw.get("data") if isinstance(raw, dict) else getattr(raw, "data", None)
        if df is None:
            return
        try:
            records = df.to_dict(orient="records")
            for r in records:
                r["symbol"] = symbol.upper()
                r["report_type"] = report_type
        except Exception:
            return
        table = f"balance_sheet_{report_type}"
        res = ingester.ingest(table, records, batch_id, self.context.run_id, symbol)
        result["rows_ingested"] += res["rows"]
        self._log_lineage(f"cophieu68_{symbol}", f"bronze_{table}", res["rows"])

    # ------------------------------------------------------------------
    # Global crawlers (run once)
    # ------------------------------------------------------------------

    def _ingest_industry_sectors(self, extractor, ingester, batch_id, result):
        rows = extractor.crawl_company_info_belong_to_industry_sectors()
        if not rows:
            return
        records = [r if isinstance(r, dict) else r.__dict__ for r in rows]
        res = ingester.ingest("industry_sectors", records, batch_id, self.context.run_id)
        result["rows_ingested"] += res["rows"]
        self._log_lineage("cophieu68_global", "bronze_industry_sectors", res["rows"])

    def _ingest_market_type_sectors(self, extractor, ingester, batch_id, result):
        rows = extractor.crawl_company_info_belong_to_market_type()
        if not rows:
            return
        records = [r if isinstance(r, dict) else r.__dict__ for r in rows]
        res = ingester.ingest("market_type_sectors", records, batch_id, self.context.run_id)
        result["rows_ingested"] += res["rows"]
        self._log_lineage("cophieu68_global", "bronze_market_type_sectors", res["rows"])

    def _ingest_industry_info(self, extractor, ingester, batch_id, result, type_info):
        raw = extractor.crawl_industry_info(type_info=type_info)
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
        res = ingester.ingest(table, records, batch_id, self.context.run_id)
        result["rows_ingested"] += res["rows"]
        self._log_lineage("cophieu68_global", f"bronze_{table}", res["rows"])

    # ------------------------------------------------------------------
    # Main execute
    # ------------------------------------------------------------------

    def execute(self) -> Dict[str, Any]:
        from shared.logger.python_main_logger import logger_manager

        self.logger.info(
            "[BronzeExecutor] Starting bronze phase symbols=%s", self.context.symbols
        )
        result = {
            "phase": "bronze",
            "symbols_processed": 0,
            "rows_ingested": 0,
            "dq_issues": 0,
            "errors": 0,
        }

        polars_engine = build_polars_engine(self.config)
        governance_logger = logger_manager.get_logger("logger.governance.data_quality")
        ingester = BronzePolarsIngester(
            engine=polars_engine,
            base_path=LAKEHOUSE_BASE,
            governance_logger=governance_logger,
        )
        extractor    = build_extractor(self.config)
        global_batch = make_batch_id("GLOBAL")

        # ── Per-symbol ────────────────────────────────────────────────
        for symbol in self.context.symbols:
            batch_id = make_batch_id(symbol)
            self.logger.info("[BronzeExecutor] symbol=%s batch=%s", symbol, batch_id)
            symbol_ok = True
            steps = [
                ("trading_data",              lambda: self._ingest_trading_data(extractor, ingester, symbol, batch_id, result)),
                ("company_profile",           lambda: self._ingest_company_profile(extractor, ingester, symbol, batch_id, result)),
                ("financial_ratios",          lambda: self._ingest_financial_ratios(extractor, ingester, symbol, batch_id, result)),
                ("financial_report_summary",  lambda: self._ingest_financial_report_summary(extractor, ingester, symbol, batch_id, result)),
                ("business_plan",             lambda: self._ingest_business_plan(extractor, ingester, symbol, batch_id, result)),
                ("income_statement_quarter",  lambda: self._ingest_income_statement(extractor, ingester, symbol, batch_id, result, "quarter")),
                ("income_statement_year",     lambda: self._ingest_income_statement(extractor, ingester, symbol, batch_id, result, "year")),
                ("balance_sheet_quarter",     lambda: self._ingest_balance_sheet(extractor, ingester, symbol, batch_id, result, "quarter")),
                ("balance_sheet_year",        lambda: self._ingest_balance_sheet(extractor, ingester, symbol, batch_id, result, "year")),
            ]
            for step_name, fn in steps:
                try:
                    fn()
                except Exception as exc:
                    from platforms.processing.base_processing_subsystem import ErrorLevel
                    self.logger.error("[Bronze] %s/%s failed: %s", symbol, step_name, exc)
                    self.context.error_log.add(ErrorLevel.WARNING, f"Bronze {symbol}/{step_name}: {exc}")
                    symbol_ok = False
            result["symbols_processed"] += 1
            if not symbol_ok:
                result["errors"] += 1

        # ── Global (run once) ─────────────────────────────────────────
        try:
            self._ingest_industry_sectors(extractor, ingester, global_batch, result)
            self._ingest_market_type_sectors(extractor, ingester, global_batch, result)
            for type_info in ("summary_info", "financial_info", "fund_info"):
                self._ingest_industry_info(extractor, ingester, global_batch, result, type_info)
        except Exception as exc:
            from platforms.processing.base_processing_subsystem import ErrorLevel
            self.logger.error("[Bronze] Global crawlers failed: %s", exc)
            self.context.error_log.add(ErrorLevel.ERROR, f"Bronze global crawl failed: {exc}")
            result["errors"] += 1

        self.logger.info(
            "[BronzeExecutor] Done: rows=%d symbols=%d errors=%d",
            result["rows_ingested"], result["symbols_processed"], result["errors"],
        )
        return result
