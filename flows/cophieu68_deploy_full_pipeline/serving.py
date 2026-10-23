"""
serving.py — Serving Phase
===========================
Silver Parquet (MinIO) → staging PostgreSQL → normalized PostgreSQL.

Luồng mỗi bảng:
  1. PolarsEngine.read_parquet() → list of dicts (all str)
  2. TRUNCATE + bulk INSERT vào staging.<table>
  3. SQL upsert staging → normalized.<table>
"""
from __future__ import annotations
import polars as pl
from typing import Any, Dict, List, Optional

from platforms.factory.client_factory import build_polars_engine, build_pg_writer
from shared.logger.logger_setup import logger_manager
from shared.telemetry import RECORDS_PROCESSED, span

from flows.common.base_config import BasePipelineConfig, BaseExecutor
from flows.common.context import ExecutionContext
from flows.cophieu68_deploy_full_pipeline.ingestion.dto.extract_models import (
    norm_numeric,
    norm_date,
)


class ServingProcessor:
    """Silver Parquet (MinIO) → upsert PostgreSQL normalized tables."""

    STAGING    = "staging"
    NORMALIZED = "normalized"

    # Cột số cần normalize trước khi load staging (per table)
    _NUMERIC_COLS: Dict[str, List[str]] = {
        "fact_stock_price":       ["close_price", "open_price", "high_price", "low_price",
                                   "volume", "foreign_buy", "foreign_sell", "foreign_value"],
        "fact_financial_metrics": ["reference_price", "open_price", "high_price", "low_price",
                                   "volume", "book_value", "eps", "pe", "pb", "roe", "roa",
                                   "beta", "market_cap", "listed_volume", "avg_volume_52w",
                                   "debt", "equity", "debt_to_equity", "equity_to_assets", "cash"],
        "fact_business_plan":     ["plan_revenue", "pass_revenue", "plan_profit", "pass_profit"],
        "dim_company":            ["listed_volume_initial", "listed_volume",
                                   "circulating_volume", "market_capitalization"],
    }

    # Cột ngày cần normalize về ISO YYYY-MM-DD (per table)
    _DATE_COLS: Dict[str, List[str]] = {
        "fact_stock_price":  ["date"],
        "dim_company":       ["established_date", "listed_date", "effective_date"],
        "dim_industry":      ["effective_date"],
    }

    def __init__(
        self,
        pg_writer,
        polars_engine,
        lakehouse_base: str = "s3://lakehouse",
        logger=None,
    ) -> None:
        self.pg     = pg_writer
        self.polars = polars_engine
        self.base   = lakehouse_base.rstrip("/")
        self.logger = logger or logger_manager.get_logger(__name__)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _stg(self, name: str) -> str:
        return f"{self.STAGING}.{name}"

    def _norm(self, name: str) -> str:
        return f"{self.NORMALIZED}.{name}"

    def _read_silver(self, silver_table: str) -> Optional[List[Dict[str, Any]]]:
        """Đọc silver/<table>/**/*.parquet → list of dicts (all str), normalize số + ngày."""


        path = f"{self.base}/silver/{silver_table}/**/*.parquet"
        try:
            df = self.polars.read_parquet(path)
            if df.is_empty():
                return None
            str_exprs = [pl.col(c).cast(pl.Utf8, strict=False).alias(c) for c in df.columns]
            df = df.with_columns(str_exprs)
            records = df.to_pandas().where(lambda x: x.notna(), None).to_dict("records")
        except Exception as exc:
            self.logger.warning("[Serving] Read silver/%s failed: %s", silver_table, exc)
            return None

        # Normalize số và ngày ngay tại đây (dùng helpers từ dto layer)
        num_cols = set(self._NUMERIC_COLS.get(silver_table, []))
        date_cols = set(self._DATE_COLS.get(silver_table, []))
        if not num_cols and not date_cols:
            return records

        cleaned = []
        for rec in records:
            r = dict(rec)
            for col in num_cols.intersection(r):
                r[col] = norm_numeric(r[col])
            for col in date_cols.intersection(r):
                r[col] = norm_date(r[col])
            cleaned.append(r)
        return cleaned

    def _load_staging(self, table: str, records: List[Dict[str, Any]]) -> int:
        """TRUNCATE + bulk INSERT records vào staging.{table}."""
        self.pg.execute(f"TRUNCATE TABLE {self._stg(table)}")
        res = self.pg.bulk_insert(self._stg(table), records)
        count = res.get("inserted_count", len(records))
        self.logger.info("[Serving] Loaded %d rows → staging.%s", count, table)
        return count

    # ------------------------------------------------------------------
    # Per-table: staging → normalized
    # ------------------------------------------------------------------

    def sync_fact_stock_price(self, run_id: str) -> Dict[str, Any]:
        """
        Sync fact_stock_price từ staging sang normalized (Partitioned Table).
        
        [ST07 Optimization]:
        ON CONFLICT sử dụng Composite Key (trade_key, trade_date) đúng theo
        ràng buộc Declarative Range Partitioning của PostgreSQL V4 migration.
        """
        records = self._read_silver("fact_stock_price")
        if not records:
            return {"rows_in": 0, "rows_out": 0, "table": "fact_stock_price"}
        rows_in = self._load_staging("fact_stock_price", records)
        
        sql = f"""
        INSERT INTO {self._norm("fact_stock_price")}
            (trade_key, symbol, trade_date, close_price, open_price,
             high_price, low_price, volume, foreign_buy, foreign_sell,
             foreign_net_value, year, month, _ingested_at, _pipeline_run_id)
        SELECT DISTINCT ON (trade_key, NULLIF("date",'None')::DATE)
            trade_key,
            UPPER(symbol),
            NULLIF("date",'None')::DATE,
            NULLIF(close_price,'None')::NUMERIC(15,2),
            NULLIF(open_price,'None')::NUMERIC(15,2),
            NULLIF(high_price,'None')::NUMERIC(15,2),
            NULLIF(low_price,'None')::NUMERIC(15,2),
            NULLIF(volume,'None')::BIGINT,
            NULLIF(foreign_buy,'None')::BIGINT,
            NULLIF(foreign_sell,'None')::BIGINT,
            NULLIF(foreign_value,'None')::NUMERIC(20,2),
            EXTRACT(YEAR  FROM NULLIF("date",'None')::DATE)::SMALLINT,
            EXTRACT(MONTH FROM NULLIF("date",'None')::DATE)::SMALLINT,
            NOW(), %(run_id)s
        FROM {self._stg("fact_stock_price")}
        WHERE symbol    IS NOT NULL AND symbol    != 'None'
          AND "date"    IS NOT NULL AND "date"    != 'None'
          AND trade_key IS NOT NULL AND trade_key != 'None'
          AND NULLIF(close_price,'None')::NUMERIC > 0
        ORDER BY trade_key, NULLIF("date",'None')::DATE, _ingest_timestamp DESC NULLS LAST
        -- [ST07 Partition Conflict Resolution]:
        ON CONFLICT (trade_key, trade_date) DO UPDATE SET
            close_price=EXCLUDED.close_price, open_price=EXCLUDED.open_price,
            high_price=EXCLUDED.high_price,   low_price=EXCLUDED.low_price,
            volume=EXCLUDED.volume,           _ingested_at=EXCLUDED._ingested_at,
            _pipeline_run_id=EXCLUDED._pipeline_run_id
        """
        self.pg.execute(sql, {"run_id": run_id})
        rows_out = self._count_run(self._norm("fact_stock_price"), run_id)
        return {"rows_in": rows_in, "rows_out": rows_out, "table": "fact_stock_price"}

    def sync_dim_company(self, run_id: str) -> Dict[str, Any]:
        """
        Sync dim_company từ staging sang normalized qua SCD Type 2 Procedure.
        
        [ST07 Optimization]:
        Gọi Stored Procedure sp_merge_dim_company_scd2 để xử lý atomic merge,
        bảo toàn lịch sử thay đổi và tối ưu round-trip DB.
        """
        records = self._read_silver("dim_company")
        if not records:
            return {"rows_in": 0, "rows_out": 0, "table": "dim_company"}
        rows_in = self._load_staging("dim_company", records)
        
        # Thực thi Stored Procedure SCD2
        call_sql = "CALL normalized.sp_merge_dim_company_scd2(%(run_id)s);"
        res = self.pg.execute(call_sql, {"run_id": run_id})
        if not res.get("ok"):
            self.logger.error("[Serving] SCD2 Procedure failed: %s", res.get("error"))
            
        rows_out = self._count_run(self._norm("dim_company"), run_id)
        return {"rows_in": rows_in, "rows_out": rows_out, "table": "dim_company"}

    def sync_fact_financial_metrics(self, run_id: str) -> Dict[str, Any]:
        records = self._read_silver("fact_financial_metrics")
        if not records:
            return {"rows_in": 0, "rows_out": 0, "table": "fact_financial_metrics"}
        rows_in = self._load_staging("fact_financial_metrics", records)
        sql = f"""
        INSERT INTO {self._norm("fact_financial_metrics")}
            (financial_ratio_key, symbol, reference_price, open_price,
             high_price, low_price, volume, book_value, eps, pe, pb,
             roe, roa, beta, market_cap, listed_volume, avg_volume_52w,
             debt, equity, debt_to_equity, equity_to_assets, cash,
             update_time, _ingested_at, _pipeline_run_id)
        SELECT DISTINCT ON (financial_ratio_key)
            financial_ratio_key, UPPER(symbol),
            NULLIF(reference_price,'None')::NUMERIC(15,2),
            NULLIF(open_price,'None')::NUMERIC(15,2),
            NULLIF(high_price,'None')::NUMERIC(15,2),
            NULLIF(low_price,'None')::NUMERIC(15,2),
            NULLIF(volume,'None')::BIGINT,
            NULLIF(book_value,'None')::NUMERIC(15,2),
            NULLIF(eps,'None')::NUMERIC(15,4),
            NULLIF(pe,'None')::NUMERIC(10,2),
            NULLIF(pb,'None')::NUMERIC(10,2),
            NULLIF(roe,'None')::NUMERIC(10,4),
            NULLIF(roa,'None')::NUMERIC(10,4),
            NULLIF(beta,'None')::NUMERIC(10,4),
            NULLIF(market_cap,'None')::NUMERIC(25,2),
            NULLIF(listed_volume,'None')::BIGINT,
            NULLIF(avg_volume_52w,'None')::BIGINT,
            NULLIF(debt,'None')::NUMERIC(20,2),
            NULLIF(equity,'None')::NUMERIC(20,2),
            NULLIF(debt_to_equity,'None')::NUMERIC(10,4),
            NULLIF(equity_to_assets,'None')::NUMERIC(10,4),
            NULLIF(cash,'None')::NUMERIC(20,2),
            NOW(), NOW(), %(run_id)s
        FROM {self._stg("fact_financial_metrics")}
        WHERE symbol IS NOT NULL AND symbol != 'None'
          AND financial_ratio_key IS NOT NULL AND financial_ratio_key != 'None'
        ORDER BY financial_ratio_key, _ingest_timestamp DESC NULLS LAST
        ON CONFLICT (financial_ratio_key) DO UPDATE SET
            eps=EXCLUDED.eps, pe=EXCLUDED.pe, pb=EXCLUDED.pb,
            roe=EXCLUDED.roe, roa=EXCLUDED.roa, market_cap=EXCLUDED.market_cap,
            _ingested_at=EXCLUDED._ingested_at, _pipeline_run_id=EXCLUDED._pipeline_run_id
        """
        self.pg.execute(sql, {"run_id": run_id})
        rows_out = self._count_run(self._norm("fact_financial_metrics"), run_id)
        return {"rows_in": rows_in, "rows_out": rows_out, "table": "fact_financial_metrics"}

    def sync_fact_business_plan(self, run_id: str) -> Dict[str, Any]:
        records = self._read_silver("fact_business_plan")
        if not records:
            return {"rows_in": 0, "rows_out": 0, "table": "fact_business_plan"}
        rows_in = self._load_staging("fact_business_plan", records)
        sql = f"""
        INSERT INTO {self._norm("fact_business_plan")}
            (plan_key, symbol, year, plan_revenue, pass_revenue,
             plan_profit, pass_profit,
             revenue_completion_rate, profit_completion_rate,
             update_time, _ingested_at, _pipeline_run_id)
        SELECT DISTINCT ON (plan_key)
            plan_key, UPPER(symbol),
            NULLIF(year,'None')::SMALLINT,
            NULLIF(plan_revenue,'None')::NUMERIC(20,2),
            NULLIF(pass_revenue,'None')::NUMERIC(20,2),
            NULLIF(plan_profit,'None')::NUMERIC(20,2),
            NULLIF(pass_profit,'None')::NUMERIC(20,2),
            CASE WHEN NULLIF(plan_revenue,'None')::NUMERIC > 0
                 THEN ROUND(NULLIF(pass_revenue,'None')::NUMERIC /
                            NULLIF(plan_revenue,'None')::NUMERIC, 4) END,
            CASE WHEN NULLIF(plan_profit,'None')::NUMERIC > 0
                 THEN ROUND(NULLIF(pass_profit,'None')::NUMERIC /
                            NULLIF(plan_profit,'None')::NUMERIC, 4) END,
            NOW(), NOW(), %(run_id)s
        FROM {self._stg("fact_business_plan")}
        WHERE symbol IS NOT NULL AND symbol != 'None'
          AND plan_key IS NOT NULL AND plan_key != 'None'
          AND NULLIF(year,'None') IS NOT NULL
        ORDER BY plan_key, _ingest_timestamp DESC NULLS LAST
        ON CONFLICT (plan_key) DO UPDATE SET
            plan_revenue=EXCLUDED.plan_revenue, pass_revenue=EXCLUDED.pass_revenue,
            plan_profit=EXCLUDED.plan_profit,   pass_profit=EXCLUDED.pass_profit,
            revenue_completion_rate=EXCLUDED.revenue_completion_rate,
            profit_completion_rate=EXCLUDED.profit_completion_rate,
            update_time=EXCLUDED.update_time,
            _ingested_at=EXCLUDED._ingested_at, _pipeline_run_id=EXCLUDED._pipeline_run_id
        """
        self.pg.execute(sql, {"run_id": run_id})
        rows_out = self._count_run(self._norm("fact_business_plan"), run_id)
        return {"rows_in": rows_in, "rows_out": rows_out, "table": "fact_business_plan"}

    def sync_dim_industry(self, run_id: str) -> Dict[str, Any]:
        records = self._read_silver("dim_industry")
        if not records:
            return {"rows_in": 0, "rows_out": 0, "table": "dim_industry"}
        rows_in = self._load_staging("dim_industry", records)
        sql = (
            f"INSERT INTO {self._norm('dim_industry')}"
            r"""
            (industry_sk, industry_code, industry_name,
             effective_date, end_date, is_current, _row_hash,
             _ingested_at, _pipeline_run_id)
        SELECT DISTINCT ON (industry_sk)
            industry_sk,
            NULLIF(industry_code,'None'), NULLIF(industry_name,'None'),
            CASE WHEN effective_date ~ '^\d{4}-\d{2}-\d{2}$'
                 THEN effective_date::DATE ELSE CURRENT_DATE END,
            NULL::DATE, TRUE,
            NULLIF(_row_hash,'None'),
            NOW(), %(run_id)s"""
            + f" FROM {self._stg('dim_industry')}"
            + r"""
        WHERE industry_code IS NOT NULL AND industry_code != 'None'
          AND industry_sk IS NOT NULL AND industry_sk != 'None'
        ORDER BY industry_sk, _ingest_timestamp DESC NULLS LAST
        ON CONFLICT (industry_sk) DO UPDATE SET
            industry_name=EXCLUDED.industry_name,
            _ingested_at=EXCLUDED._ingested_at, _pipeline_run_id=EXCLUDED._pipeline_run_id
        """
        )
        self.pg.execute(sql, {"run_id": run_id})
        rows_out = self._count_run(self._norm("dim_industry"), run_id)
        return {"rows_in": rows_in, "rows_out": rows_out, "table": "dim_industry"}

    def sync_dim_market_type(self, run_id: str) -> Dict[str, Any]:
        records = self._read_silver("dim_market_type")
        if not records:
            return {"rows_in": 0, "rows_out": 0, "table": "dim_market_type"}
        rows_in = self._load_staging("dim_market_type", records)
        sql = f"""
        INSERT INTO {self._norm("dim_market_type")}
            (market_key, market_type, market_name, update_time, _ingested_at)
        SELECT DISTINCT ON (market_key)
            market_key,
            NULLIF(market_type_code,'None'), NULLIF(market_type_name,'None'),
            NOW(), NOW()
        FROM {self._stg("dim_market_type")}
        WHERE market_key IS NOT NULL AND market_key != 'None'
        ORDER BY market_key, _ingest_timestamp DESC NULLS LAST
        ON CONFLICT (market_key) DO UPDATE SET
            market_name=EXCLUDED.market_name, update_time=EXCLUDED.update_time
        """
        self.pg.execute(sql, {"run_id": run_id})
        rows_out = self._count_run(self._norm("dim_market_type"), None)
        return {"rows_in": rows_in, "rows_out": rows_out, "table": "dim_market_type"}

    def _count_run(self, table: str, run_id: Optional[str]) -> int:
        if run_id:
            res = self.pg.query(
                f"SELECT COUNT(*) AS n FROM {table} WHERE _pipeline_run_id = %(r)s",
                {"r": run_id},
            )
        else:
            res = self.pg.query(f"SELECT COUNT(*) AS n FROM {table}", None)
        return res.get("results", [{}])[0].get("n", 0)

    # ------------------------------------------------------------------
    # Run all
    # ------------------------------------------------------------------

    def run_all(self, run_id: str) -> Dict[str, Any]:
        syncs = [
            ("fact_stock_price",       lambda: self.sync_fact_stock_price(run_id)),
            ("dim_company",            lambda: self.sync_dim_company(run_id)),
            ("fact_financial_metrics", lambda: self.sync_fact_financial_metrics(run_id)),
            ("fact_business_plan",     lambda: self.sync_fact_business_plan(run_id)),
            ("dim_industry",           lambda: self.sync_dim_industry(run_id)),
            ("dim_market_type",        lambda: self.sync_dim_market_type(run_id)),
        ]
        results: Dict[str, Any] = {
            "run_id": run_id, "tables": {},
            "total_rows_in": 0, "total_rows_out": 0, "errors": 0,
        }
        for name, fn in syncs:
            try:
                with span("serving.sync", table=name):
                    res = fn()
                RECORDS_PROCESSED.labels("serving", name).inc(res.get("rows_out", 0))
                results["tables"][name]      = res
                results["total_rows_in"]    += res.get("rows_in",  0)
                results["total_rows_out"]   += res.get("rows_out", 0)
                self.logger.info(
                    "[Serving] %s: in=%d out=%d", name,
                    res.get("rows_in", 0), res.get("rows_out", 0),
                )
            except Exception as exc:
                self.logger.error("[Serving] %s failed: %s", name, exc)
                results["tables"][name] = {"error": str(exc)}
                results["errors"] += 1
        return results


# ---------------------------------------------------------------------------
# ServingExecutor — orchestrate silver → PG
# ---------------------------------------------------------------------------

class ServingExecutor(BaseExecutor):
    """Silver → PostgreSQL sync executor."""

    def __init__(self, context: ExecutionContext, config: BasePipelineConfig) -> None:
        self.context = context
        self.config  = config
        self.logger  = context.logger

    def execute(self) -> Dict[str, Any]:
        from platforms.processing.base_processing_subsystem import ErrorLevel

        self.logger.info("[ServingExecutor] Starting serving phase")
        result = {"phase": "serving", "tables_synced": 0, "errors": 0, "details": {}}

        pg = build_pg_writer(**self.config.pg_conn_params)
        if pg is None:
            self.logger.warning("[ServingExecutor] No PG credentials — skipping")
            return {**result, "skipped": True, "reason": "missing_pg_credentials"}

        polars_engine = build_polars_engine(**self.config.polars_build_params)
        proc = ServingProcessor(
            pg_writer=pg,
            polars_engine=polars_engine,
            lakehouse_base=self.config.lakehouse_base,
            logger=self.logger,
        )

        try:
            sync_result = proc.run_all(run_id=self.context.run_id)
            result["details"]      = sync_result["tables"]
            result["tables_synced"] = sync_result["total_rows_out"]
            result["errors"]       += sync_result["errors"]
        except Exception as exc:
            self.logger.error("[ServingExecutor] Error: %s", exc)
            self.context.error_log.add(ErrorLevel.ERROR, f"Serving sync failed: {exc}")
            result["errors"] += 1
        finally:
            pg.close()

        self.logger.info("[ServingExecutor] Done tables_synced=%d errors=%d",
                         result["tables_synced"], result["errors"])
        return result
