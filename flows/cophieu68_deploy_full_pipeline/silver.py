"""
silver.py — Silver Phase Module
================================
Bronze Parquet (MinIO) → Polars transform → Silver Parquet (MinIO).

Luồng:
  1. PolarsEngine.read_parquet()  đọc bronze/**/*.parquet từ S3
  2. Polars transform             deduplicate, type-cast, tạo surrogate keys
  3. PolarsEngine.write_parquet() ghi silver/**/*.parquet lên S3

Dùng:
  - platforms/processing/polars/  → đọc/ghi Parquet S3

Không chứa:
  - DuckDB engine (đã bỏ — PolarsEngine đọc S3 Parquet trực tiếp)
  - SQLMesh engine (gold.py không còn dùng)
  - Crawl web (bronze.py)
  - PostgreSQL sync (serving.py)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from platforms.factory.client_factory import build_polars_engine

from flows.common.base_executor import BaseExecutor
from flows.common.context import ExecutionContext
from flows.cophieu68_deploy_full_pipeline.pipeline_config import Cophieu68PipelineConfig


class SilverProcessor:
    """
    Đọc Bronze Parquet → chạy dbt transform → ghi Silver Parquet.

    Mỗi transform_* method = 1 bảng, độc lập nhau.
    """

    def __init__(
        self,
        polars_engine,
        base_path: str = "s3://lakehouse",
    ) -> None:
        self.polars     = polars_engine
        self.base        = base_path
        self.logger     = polars_engine.logger

        from platforms.processing.base_processing_subsystem import SurrogateKeyGenerator
        self.sk_gen = SurrogateKeyGenerator(prefix="STK_", key_length=32)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_bronze(
        self,
        table_name: str,
        target_date: str,
        allow_fallback: bool = True,
    ) -> Optional[Any]:
        """
        Đọc bronze/<table_name>/ qua PolarsEngine (S3/MinIO).
        Ưu tiên partition exact date, fallback full scan.
        """
        import polars as pl

        exact    = f"{self.base}/bronze/{table_name}/ingest_date={target_date}/*.parquet"
        fallback = f"{self.base}/bronze/{table_name}/**/*.parquet"

        paths = [(exact, "exact")]
        if allow_fallback:
            paths.append((fallback, "fallback"))
        for path, label in paths:
            try:
                df = self.polars.read_parquet(path)
                if not df.is_empty():
                    if label == "fallback":
                        self.logger.warning(
                            "[Silver] bronze/%s — no data for %s, used fallback (%d rows)",
                            table_name, target_date, len(df),
                        )
                    return self._cast_non_numeric(df)
            except Exception as exc:
                self.logger.debug("[Silver] read %s (%s) failed: %s", table_name, label, exc)

        if allow_fallback:
            self.logger.warning("[Silver] bronze/%s is empty — skipping", table_name)
        else:
            self.logger.warning(
                "[Silver] bronze/%s has no partition for %s — skipping stale fallback",
                table_name, target_date,
            )
        return None

    def _cast_non_numeric(self, df: Any) -> Any:
        """Cast non-numeric columns sang Utf8 để tránh schema conflicts."""
        import polars as pl
        NUMERIC = (
            pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
            pl.Float32, pl.Float64,
        )
        exprs = [
            pl.col(c).cast(pl.Utf8, strict=False).alias(c)
            for c, dt in zip(df.columns, df.dtypes)
            if not isinstance(dt, NUMERIC) and dt not in (pl.Utf8, pl.String)
        ]
        return df.with_columns(exprs) if exprs else df

    def _write_silver(
        self,
        df: Any,
        silver_table: str,
        partition_by: Optional[List[str]] = None,
    ) -> str:
        path = f"{self.base}/silver/{silver_table}/"
        self.polars.write_parquet(df=df, target_path=path, partition_by=partition_by)
        self.logger.info("[Silver] Wrote %d rows → %s", len(df), path)
        return path

    def _add_audit(self, df: Any, run_id: str) -> Any:
        import polars as pl
        return df.with_columns([
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingested_at"),
            pl.lit(run_id).alias("_pipeline_run_id"),
        ])

    def _dedup(self, df: Any, keys: List[str], run_id: str, source: str) -> Any:
        """Dedup qua DeduplicationEngine (pandas bridge)."""
        import polars as pl
        from platforms.processing.base_processing_subsystem import (
            DeduplicationEngine,
            DeduplicationStrategy,
        )
        df_pd = df.to_pandas()
        valid_keys = [k for k in keys if k in df_pd.columns]
        if not valid_keys:
            self.logger.warning("[Silver] Dedup(%s): keys %s not found", source, keys)
            return df
        engine = DeduplicationEngine(
            keys=valid_keys,
            strategy=DeduplicationStrategy.KEEP_LAST,
            tiebreaker_col="_ingest_timestamp",
        )
        deduped, stats = engine.deduplicate_dataframe(df=df_pd, source=source, run_id=run_id)
        self.logger.info("[Silver] Dedup(%s): %s", source, stats)
        return pl.from_pandas(deduped)

    def _safe_cast(self, df: Any, col: str, dtype: Any) -> Any:
        import polars as pl
        if col not in df.columns:
            return df
        return df.with_columns(pl.col(col).cast(dtype, strict=False).alias(col))

    # ------------------------------------------------------------------
    # Per-table Polars transforms
    # ------------------------------------------------------------------

    def transform_stock_prices(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/stock_prices → silver/fact_stock_price"""
        import polars as pl
        df = self._read_bronze("stock_prices", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)
        df = self._dedup(df, keys=["symbol", "date"], run_id=run_id,
                         source="bronze.stock_prices")
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.col("date")], separator="|")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("trade_key")
        )
        if "ingest_date" in df.columns:
            df = df.with_columns([
                pl.col("ingest_date").str.slice(0, 4).cast(pl.Int32).alias("year"),
                pl.col("ingest_date").str.slice(5, 2).cast(pl.Int32).alias("month"),
            ])
        df = self._add_audit(df, run_id)
        parts = ["year", "month"] if "year" in df.columns and "month" in df.columns else []
        path = self._write_silver(df, "fact_stock_price", partition_by=parts)
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}

    def transform_company_profile(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/company_profile → silver/dim_company"""
        import polars as pl
        df = self._read_bronze("company_profile", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)
        df = self._dedup(df, keys=["symbol"], run_id=run_id, source="bronze.company_profile")
        df = df.with_columns(
            pl.col("symbol").map_elements(self.sk_gen.hash_key, return_dtype=pl.Utf8)
            .alias("company_key")
        )
        df = self._add_audit(df, run_id)
        path = self._write_silver(df, "dim_company")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}

    def transform_financial_ratios(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/financial_ratios → silver/fact_financial_metrics"""
        import polars as pl
        df = self._read_bronze("financial_ratios", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)
        df = self._dedup(df, keys=["symbol"], run_id=run_id, source="bronze.financial_ratios")
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.lit(target_date)], separator="|")
            .map_elements(self.sk_gen.hash_key, return_dtype=pl.Utf8)
            .alias("financial_ratio_key")
        )
        df = self._add_audit(df, run_id)
        path = self._write_silver(df, "fact_financial_metrics")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}

    def transform_financial_report(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/financial_report_summary → silver/fact_financial_report"""
        df = self._read_bronze("financial_report_summary", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)
        df = self._dedup(df, keys=["symbol", "report_type"], run_id=run_id,
                         source="bronze.financial_report_summary")
        df = self._add_audit(df, run_id)
        path = self._write_silver(df, "fact_financial_report")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}

    def transform_business_plan(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/business_plan → silver/fact_business_plan"""
        import polars as pl
        df = self._read_bronze("business_plan", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)
        rename_map = {
            source: target
            for source, target in {
                "Year": "year",
                "Plan_revenue": "plan_revenue",
                "Pass_revenue": "pass_revenue",
                "Plan_profit": "plan_profit",
                "Pass_profit": "pass_profit",
            }.items()
            if source in df.columns and target not in df.columns
        }
        if rename_map:
            df = df.rename(rename_map)
        df = self._dedup(df, keys=["symbol", "year"], run_id=run_id, source="bronze.business_plan")
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.col("year")], separator="|")
            .map_elements(self.sk_gen.hash_key, return_dtype=pl.Utf8)
            .alias("plan_key")
        )
        df = self._add_audit(df, run_id)
        path = self._write_silver(df, "fact_business_plan")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}

    def transform_income_statement(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/income_statement_* → silver/fact_income_statement"""
        import polars as pl
        dfs = []
        for rtype in ("quarter", "year"):
            df = self._read_bronze(f"income_statement_{rtype}", target_date)
            if df is not None:
                dfs.append(df)
        if not dfs:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        df = pl.concat(dfs, how="diagonal")
        rows_in = len(df)
        df = self._dedup(df, keys=["symbol", "report_type"], run_id=run_id,
                         source="bronze.income_statement")
        df = self._add_audit(df, run_id)
        path = self._write_silver(df, "fact_income_statement")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}

    def transform_balance_sheet(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/balance_sheet_* → silver/fact_balance_sheet"""
        import polars as pl
        dfs = []
        for rtype in ("quarter", "year"):
            df = self._read_bronze(f"balance_sheet_{rtype}", target_date)
            if df is not None:
                dfs.append(df)
        if not dfs:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        df = pl.concat(dfs, how="diagonal")
        rows_in = len(df)
        df = self._dedup(df, keys=["symbol", "report_type"], run_id=run_id,
                         source="bronze.balance_sheet")
        df = self._add_audit(df, run_id)
        path = self._write_silver(df, "fact_balance_sheet")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}

    def transform_industry_sectors(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/industry_sectors → silver/dim_industry"""
        import polars as pl
        df = self._read_bronze("industry_sectors", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)
        df = self._dedup(df, keys=["industry_code"], run_id=run_id, source="bronze.industry_sectors")
        df = df.with_columns(
            pl.col("industry_code")
            .map_elements(self.sk_gen.hash_key, return_dtype=pl.Utf8)
            .alias("industry_sk")
        )
        df = self._add_audit(df, run_id)
        path = self._write_silver(df, "dim_industry")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}

    def transform_market_type_sectors(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/market_type_sectors → silver/dim_market_type"""
        import polars as pl
        df = self._read_bronze("market_type_sectors", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)
        df = self._dedup(df, keys=["market_type_code"], run_id=run_id,
                         source="bronze.market_type_sectors")
        df = df.with_columns(
            pl.col("market_type_code")
            .map_elements(self.sk_gen.hash_key, return_dtype=pl.Utf8)
            .alias("market_key")
        )
        df = self._add_audit(df, run_id)
        path = self._write_silver(df, "dim_market_type")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}

    def transform_industry_info(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/industry_info_* → silver/fact_industry_summary"""
        import polars as pl
        dfs = []
        for kind in ("summary_info", "financial_info", "fund_info"):
            df = self._read_bronze(
                f"industry_info_{kind}",
                target_date,
                allow_fallback=False,
            )
            if df is not None:
                dfs.append(df)
        if not dfs:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        df = pl.concat(dfs, how="diagonal")
        rows_in = len(df)
        df = self._dedup(df, keys=["_industry_key"], run_id=run_id,
                         source="bronze.industry_info")
        df = self._add_audit(df, run_id)
        path = self._write_silver(df, "fact_industry_summary")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": path}


# ---------------------------------------------------------------------------
# SilverExecutor — orchestrate silver phase
# ---------------------------------------------------------------------------

class SilverExecutor(BaseExecutor):
    """
    Orchestrate Silver phase: bronze → silver.

    Silver được materialize trực tiếp bằng các transform Polars theo từng bảng.
    """

    def __init__(
        self,
        context: ExecutionContext,
        config: "Cophieu68PipelineConfig",
    ) -> None:
        self.context = context
        self.config  = config
        self.logger  = context.logger

    def execute(self) -> Dict[str, Any]:
        from platforms.processing.base_processing_subsystem import ErrorLevel

        self.logger.info(
            "[SilverExecutor] Starting silver phase date=%s", self.context.target_date
        )
        result: Dict[str, Any] = {
            "phase": "silver", "rows_in": 0, "rows_out": 0, "errors": 0
        }

        polars_engine = build_polars_engine(**self.config.polars_build_params)
        proc = SilverProcessor(
            polars_engine=polars_engine,
            base_path=self.config.lakehouse_base,
        )

        transforms = [
            ("stock_prices",        "fact_stock_price",        lambda: proc.transform_stock_prices(self.context.target_date, self.context.run_id)),
            ("company_profile",     "dim_company",             lambda: proc.transform_company_profile(self.context.target_date, self.context.run_id)),
            ("financial_ratios",    "fact_financial_metrics",  lambda: proc.transform_financial_ratios(self.context.target_date, self.context.run_id)),
            ("financial_report",    "fact_financial_report",   lambda: proc.transform_financial_report(self.context.target_date, self.context.run_id)),
            ("business_plan",       "fact_business_plan",      lambda: proc.transform_business_plan(self.context.target_date, self.context.run_id)),
            ("income_statement",    "fact_income_statement",   lambda: proc.transform_income_statement(self.context.target_date, self.context.run_id)),
            ("balance_sheet",       "fact_balance_sheet",      lambda: proc.transform_balance_sheet(self.context.target_date, self.context.run_id)),
            ("industry_sectors",    "dim_industry",            lambda: proc.transform_industry_sectors(self.context.target_date, self.context.run_id)),
            ("market_type_sectors", "dim_market_type",         lambda: proc.transform_market_type_sectors(self.context.target_date, self.context.run_id)),
            ("industry_info",       "fact_industry_summary",   lambda: proc.transform_industry_info(self.context.target_date, self.context.run_id)),
        ]

        for bronze_src, silver_tgt, fn in transforms:
            try:
                res = fn()
                result["rows_in"]  += res.get("rows_in",  0)
                result["rows_out"] += res.get("rows_out", 0)
                if res.get("rows_out", 0) > 0:
                    self.context.metadata_repo.log_lineage(
                        run_id=self.context.run_id,
                        source_layer="bronze", source_table=f"bronze_{bronze_src}",
                        target_layer="silver", target_table=f"silver_{silver_tgt}",
                        operation="MERGE", rows_affected=res["rows_out"],
                    )
                self.logger.info(
                    "[Silver] %s → %s: in=%d out=%d",
                    bronze_src, silver_tgt,
                    res.get("rows_in", 0), res.get("rows_out", 0),
                )
            except Exception as exc:
                self.logger.error("[Silver] %s failed: %s", bronze_src, exc)
                self.context.error_log.add(
                    ErrorLevel.ERROR,
                    f"Silver {bronze_src} → {silver_tgt} failed: {exc}",
                )
                result["errors"] += 1

        self.logger.info(
            "[SilverExecutor] Done rows_out=%d errors=%d",
            result["rows_out"], result["errors"],
        )
        return result
