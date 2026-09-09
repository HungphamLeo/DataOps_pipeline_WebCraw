"""
silver.py — Silver Phase Module
================================
Đọc Bronze Parquet từ MinIO → transform + dedup + type-cast → ghi Silver Parquet lên MinIO.

Dùng:
  - platforms/processing/duckdb/  → đọc Parquet qua S3
  - platforms/processing/polars/  → transform + ghi Parquet
  - platforms/processing/base_processing_subsystem/ → dedup, surrogate key

Không chứa:
  - Kết nối PostgreSQL (thuộc serving.py)
  - Crawl web (thuộc bronze.py)
  - CLI / orchestration (thuộc run.py)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from flows.cophieu68_deploy_full_pipeline.context import (
    ExecutionContext,
    LAKEHOUSE_BASE,
)
from flows.cophieu68_deploy_full_pipeline.builders import (
    build_polars_engine,
    build_duckdb_engine,
)


class SilverProcessor:
    """
    Đọc Bronze Parquet → transform → ghi Silver Parquet trên MinIO.

    Mỗi method = 1 bảng. Độc lập nhau, lỗi 1 bảng không ảnh hưởng bảng khác.
    """

    def __init__(
        self,
        duckdb_engine,
        polars_engine,
        base_path: str = LAKEHOUSE_BASE,
    ) -> None:
        self.duck   = duckdb_engine
        self.polars = polars_engine
        self.base   = base_path
        self.logger = polars_engine.logger

        from platforms.processing.base_processing_subsystem import SurrogateKeyGenerator
        self.sk_gen = SurrogateKeyGenerator(prefix="STK_", key_length=32)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_bronze(self, table_name: str, target_date: str) -> Optional[Any]:
        """
        Đọc bronze/<table_name>/ qua DuckDB httpfs.
        Ưu tiên exact partition ingest_date=<target_date>, fallback full scan.
        Cast tất cả cột non-numeric sang Utf8 để tránh lỗi str.slice() downstream.
        """
        import polars as pl

        def _query(glob: str) -> Optional[Any]:
            try:
                df = self.duck.query_to_polars(
                    f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
                ).collect()
                return df if not df.is_empty() else None
            except Exception as exc:
                self.logger.debug("[Silver] read_parquet(%s) failed: %s", glob, exc)
                return None

        def _cast_to_utf8(df: Any) -> Any:
            """Giữ numeric, cast còn lại sang Utf8."""
            KEEP = (
                pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                pl.Float32, pl.Float64,
            )
            exprs = [
                pl.col(c).cast(pl.Utf8, strict=False).alias(c)
                for c, dt in zip(df.columns, df.dtypes)
                if not isinstance(dt, KEEP) and dt not in (pl.Utf8, pl.String)
            ]
            return df.with_columns(exprs) if exprs else df

        exact = f"{self.base}/bronze/{table_name}/ingest_date={target_date}/*.parquet"
        self.logger.info("[Silver] Reading bronze/%s (date=%s)", table_name, target_date)
        df = _query(exact)
        if df is not None:
            return _cast_to_utf8(df)

        fallback = f"{self.base}/bronze/{table_name}/**/*.parquet"
        self.logger.warning(
            "[Silver] No data for %s on %s — fallback full scan", table_name, target_date
        )
        df = _query(fallback)
        if df is not None:
            self.logger.info("[Silver] Fallback found %d rows in bronze/%s", len(df), table_name)
            return _cast_to_utf8(df)

        self.logger.warning("[Silver] bronze/%s is empty — skipping", table_name)
        return None

    def _write_silver(self, df: Any, silver_table: str,
                      partition_by: Optional[List[str]] = None) -> str:
        path = f"{self.base}/silver/{silver_table}/"
        self.polars.write_parquet(
            df=df,
            target_path=path,
            partition_by=partition_by if partition_by else None,
        )
        self.logger.info("[Silver] Wrote %d rows → %s", len(df), path)
        return path

    def _add_audit(self, df: Any, run_id: str) -> Any:
        import polars as pl
        return df.with_columns([
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingested_at"),
            pl.lit(run_id).alias("_pipeline_run_id"),
        ])

    def _dedup(self, df: Any, keys: List[str], run_id: str, source: str) -> Any:
        """Dedup qua DeduplicationEngine (pandas bridge). Fallback KEEP_LAST."""
        import polars as pl
        from platforms.processing.base_processing_subsystem import (
            DeduplicationEngine,
            DeduplicationStrategy,
        )

        df_pd = df.to_pandas()
        valid_keys = [k for k in keys if k in df_pd.columns]
        if not valid_keys:
            self.logger.warning(
                "[Silver] Dedup(%s): keys %s not found, skipping", source, keys
            )
            return df

        engine = DeduplicationEngine(
            keys=valid_keys,
            strategy=DeduplicationStrategy.KEEP_LAST,
            tiebreaker_col="_ingest_timestamp",
        )
        deduped, stats = engine.deduplicate_dataframe(df=df_pd, source=source, run_id=run_id)
        self.logger.info("[Silver] Dedup(%s) stats: %s", source, stats)
        return pl.from_pandas(deduped)

    def _safe_cast(self, df: Any, col: str, dtype: Any) -> Any:
        import polars as pl
        if col not in df.columns:
            return df
        return df.with_columns(pl.col(col).cast(dtype, strict=False).alias(col))

    # ------------------------------------------------------------------
    # Per-table transforms
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
        parts = (["year", "month"]
                 if "year" in df.columns and "month" in df.columns else [])
        silver_path = self._write_silver(df, "fact_stock_price", partition_by=parts)
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_company_profile(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/company_profile → silver/dim_company"""
        import polars as pl

        df = self._read_bronze("company_profile", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        df = self._dedup(df, keys=["symbol"], run_id=run_id,
                         source="bronze.company_profile")
        df = df.with_columns(
            pl.col("symbol")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("company_key")
        )
        today = datetime.now(timezone.utc).date().isoformat()
        df = df.with_columns([
            pl.lit(today).alias("effective_date"),
            pl.lit(None).cast(pl.Utf8).alias("end_date"),
            pl.lit(True).alias("is_current"),
        ])
        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "dim_company")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_financial_ratios(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/financial_ratios → silver/fact_financial_metrics"""
        import polars as pl

        df = self._read_bronze("financial_ratios", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        df = self._dedup(df, keys=["symbol"], run_id=run_id,
                         source="bronze.financial_ratios")
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.lit(target_date)], separator="|")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("financial_ratio_key")
        )
        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "fact_financial_metrics")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_financial_report(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/financial_report_summary → silver/fact_financial_report"""
        import polars as pl

        df = self._read_bronze("financial_report_summary", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        dedup_keys = [k for k in ["symbol", "report_type"] if k in df.columns]
        df = self._dedup(df, keys=dedup_keys, run_id=run_id,
                         source="bronze.financial_report_summary")
        df = self._add_audit(df, run_id)
        parts = ["ingest_date"] if "ingest_date" in df.columns else []
        silver_path = self._write_silver(df, "fact_financial_report", partition_by=parts)
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_business_plan(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/business_plan → silver/fact_business_plan"""
        import polars as pl

        df = self._read_bronze("business_plan", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)

        if "Year" in df.columns and "year" not in df.columns:
            df = df.rename({"Year": "year"})

        df = self._dedup(df, keys=["symbol", "year"], run_id=run_id,
                         source="bronze.business_plan")
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.col("year")], separator="|")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("plan_key")
        )
        for col in ("plan_revenue", "pass_revenue", "plan_profit", "pass_profit",
                    "Plan_revenue", "Pass_revenue", "Plan_profit", "Pass_profit"):
            df = self._safe_cast(df, col, pl.Float64)
        df = df.rename({c: c.lower() for c in df.columns})
        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "fact_business_plan")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_income_statement(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/income_statement_{quarter,year} → silver/fact_income_statement"""
        import polars as pl

        frames = []
        for rtype, label in (("quarter", "QUARTERLY"), ("year", "ANNUALLY")):
            df = self._read_bronze(f"income_statement_{rtype}", target_date)
            if df is not None:
                df = df.with_columns([
                    pl.lit(label).alias("time_report_type"),
                    pl.lit(rtype).alias("report_type"),
                ])
                frames.append(df)
        if not frames:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}

        df = pl.concat(frames, how="diagonal")
        rows_in = len(df)
        df = self._dedup(df, keys=["symbol", "time_report_type"], run_id=run_id,
                         source="bronze.income_statement")
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.col("time_report_type")], separator="|")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("income_key")
        )
        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        if "ingest_date" in df.columns:
            df = df.with_columns(
                pl.col("ingest_date").str.slice(0, 4).alias("year")
            )
        df = self._add_audit(df, run_id)
        parts = (["time_report_type", "year"]
                 if "time_report_type" in df.columns and "year" in df.columns else [])
        silver_path = self._write_silver(df, "fact_income_statement", partition_by=parts)
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_balance_sheet(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/balance_sheet_{quarter,year} → silver/fact_balance_sheet"""
        import polars as pl

        frames = []
        for rtype, label in (("quarter", "QUARTERLY"), ("year", "ANNUALLY")):
            df = self._read_bronze(f"balance_sheet_{rtype}", target_date)
            if df is not None:
                df = df.with_columns([
                    pl.lit(label).alias("time_report_type"),
                    pl.lit(rtype).alias("report_type"),
                ])
                frames.append(df)
        if not frames:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}

        df = pl.concat(frames, how="diagonal")
        rows_in = len(df)
        df = self._dedup(df, keys=["symbol", "time_report_type"], run_id=run_id,
                         source="bronze.balance_sheet")
        df = df.with_columns(
            pl.concat_str([pl.col("symbol"), pl.col("time_report_type")], separator="|")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("balance_key")
        )
        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        if "ingest_date" in df.columns:
            df = df.with_columns(
                pl.col("ingest_date").str.slice(0, 4).alias("year")
            )
        df = self._add_audit(df, run_id)
        parts = (["time_report_type", "year"]
                 if "time_report_type" in df.columns and "year" in df.columns else [])
        silver_path = self._write_silver(df, "fact_balance_sheet", partition_by=parts)
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_industry_sectors(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/industry_sectors → silver/dim_industry"""
        import polars as pl

        df = self._read_bronze("industry_sectors", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)
        df = self._dedup(df, keys=["industry_code", "symbol"], run_id=run_id,
                         source="bronze.industry_sectors")
        df = df.with_columns(
            pl.col("industry_code")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("industry_sk")
        )
        today = datetime.now(timezone.utc).date().isoformat()
        df = df.with_columns([
            pl.lit(today).alias("effective_date"),
            pl.lit(None).cast(pl.Utf8).alias("end_date"),
            pl.lit(True).alias("is_current"),
        ])
        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "dim_industry")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_market_type_sectors(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/market_type_sectors → silver/dim_market_type"""
        import polars as pl

        df = self._read_bronze("market_type_sectors", target_date)
        if df is None:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}
        rows_in = len(df)
        df = self._dedup(df, keys=["market_type_code"], run_id=run_id,
                         source="bronze.market_type_sectors")
        if "market_type_code" in df.columns and "market_type" not in df.columns:
            df = df.rename({"market_type_code": "market_type"})
        if "market_type_name" in df.columns and "market_name" not in df.columns:
            df = df.rename({"market_type_name": "market_name"})
        df = df.with_columns(
            pl.col("market_type")
            .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
            .alias("market_key")
        )
        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "dim_market_type")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}

    def transform_industry_info(self, target_date: str, run_id: str) -> Dict[str, Any]:
        """bronze/industry_info_{summary,financial,fund} → silver/fact_industry_summary"""
        import polars as pl

        frames = []
        for type_info in ("summary_info", "financial_info", "fund_info"):
            df = self._read_bronze(f"industry_info_{type_info}", target_date)
            if df is not None:
                df = df.with_columns(
                    pl.lit(type_info).alias("industry_metric_type")
                )
                frames.append(df)
        if not frames:
            return {"rows_in": 0, "rows_out": 0, "silver_path": None}

        df = pl.concat(frames, how="diagonal")
        rows_in = len(df)

        dedup_keys = [k for k in ["_industry_key", "industry_metric_type"] if k in df.columns]
        df = self._dedup(df, keys=dedup_keys, run_id=run_id, source="bronze.industry_info")

        key_cols = [c for c in ["_industry_key", "industry_metric_type"] if c in df.columns]
        if key_cols:
            df = df.with_columns(
                pl.concat_str([pl.col(c) for c in key_cols], separator="|")
                .map_elements(lambda s: self.sk_gen.hash_key(s), return_dtype=pl.Utf8)
                .alias("industry_summary_key")
            )
        for col in ("pe", "roa", "roe", "industry_index", "percentage_change",
                    "liquidity", "total_capital", "supply_volumn", "total_asset",
                    "total_equity", "total_liabilities",
                    "percentage_debt_on_equity", "percentage_equity_on_assets",
                    "revenue", "profit_before_tax"):
            df = self._safe_cast(df, col, pl.Float64)

        df = df.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("update_time")
        )
        df = self._add_audit(df, run_id)
        silver_path = self._write_silver(df, "fact_industry_summary")
        return {"rows_in": rows_in, "rows_out": len(df), "silver_path": silver_path}


# ---------------------------------------------------------------------------
# SilverExecutor — orchestrate all transforms
# ---------------------------------------------------------------------------

class SilverExecutor:
    """
    Chạy toàn bộ Silver transforms cho pipeline cophieu68.
    Dùng SilverProcessor + engines từ builders.
    """

    def __init__(self, context: ExecutionContext, config: Dict[str, Any]) -> None:
        self.context = context
        self.config  = config
        self.logger  = context.logger

    def execute(self) -> Dict[str, Any]:
        from platforms.processing.base_processing_subsystem import ErrorLevel

        self.logger.info(
            "[SilverExecutor] Starting silver phase date=%s", self.context.target_date
        )
        result = {"phase": "silver", "rows_in": 0, "rows_out": 0, "errors": 0}

        duck   = build_duckdb_engine(self.config)
        polars = build_polars_engine(self.config)
        proc   = SilverProcessor(
            duckdb_engine=duck,
            polars_engine=polars,
            base_path=LAKEHOUSE_BASE,
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

        try:
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
        finally:
            duck.close()

        self.logger.info(
            "[SilverExecutor] Done rows_out=%d errors=%d",
            result["rows_out"], result["errors"],
        )
        return result
