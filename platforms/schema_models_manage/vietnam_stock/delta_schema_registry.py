"""
Delta Lake Schema Registry
==========================
Định nghĩa schema cho 3 lớp Bronze / Silver / Gold của Lakehouse.

Bronze  → Raw ingestion (append-only, giữ nguyên JSON gốc + audit cols)
Silver  → Cleaned, typed, deduplicated (MERGE upsert / SCD2)
Gold    → Business-ready, Power BI optimized (pivoted, ZORDER)

Mỗi entry là dict tương thích với PySpark StructType hoặc dùng trực tiếp
để tạo Delta table bằng SQL DDL.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class ColumnDef:
    name: str
    spark_type: str          # e.g. "StringType", "DoubleType", "TimestampType"
    nullable: bool = True
    comment: str = ""


@dataclass
class DeltaTableDef:
    """Full definition of one Delta table."""
    layer: str               # bronze | silver | gold
    table_name: str
    relative_path: str       # relative to base_path, e.g. "bronze/trading_data"
    columns: List[ColumnDef] = field(default_factory=list)
    partition_by: List[str] = field(default_factory=list)
    zorder_by: List[str] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)   # for MERGE
    enable_cdf: bool = False  # Change Data Feed (CDC – Subsystem 2)
    description: str = ""

    def ddl_columns(self) -> str:
        """Return SQL column definitions string."""
        type_map = {
            "StringType":    "STRING",
            "LongType":      "BIGINT",
            "IntegerType":   "INT",
            "DoubleType":    "DOUBLE",
            "FloatType":     "FLOAT",
            "BooleanType":   "BOOLEAN",
            "TimestampType": "TIMESTAMP",
            "DateType":      "DATE",
            "DecimalType":   "DECIMAL(20,4)",
            "MapType":       "MAP<STRING,STRING>",
            "ArrayType":     "ARRAY<STRING>",
        }
        parts = []
        for c in self.columns:
            sql_type = type_map.get(c.spark_type, c.spark_type)
            null_str = "" if c.nullable else " NOT NULL"
            comment_str = f" COMMENT '{c.comment}'" if c.comment else ""
            parts.append(f"  {c.name} {sql_type}{null_str}{comment_str}")
        return ",\n".join(parts)

    def create_table_sql(self, base_path: str) -> str:
        """Generate CREATE TABLE IF NOT EXISTS SQL for Delta."""
        partition_clause = ""
        if self.partition_by:
            partition_clause = f"\nPARTITIONED BY ({', '.join(self.partition_by)})"

        tblprops = ["'delta.minReaderVersion' = '2'",
                    "'delta.minWriterVersion' = '5'"]
        if self.enable_cdf:
            tblprops.append("'delta.enableChangeDataFeed' = 'true'")

        props_clause = f"\nTBLPROPERTIES ({', '.join(tblprops)})"
        path = f"{base_path}/{self.relative_path}"

        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} (\n"
            f"{self.ddl_columns()}\n"
            f"){partition_clause}"
            f"\nUSING DELTA"
            f"\nLOCATION '{path}'"
            f"{props_clause}"
            f"\nCOMMENT '{self.description}'"
        )


# ---------------------------------------------------------------------------
# ██████╗ ██████╗  ██████╗ ███╗   ██╗███████╗███████╗
# ██╔══██╗██╔══██╗██╔═══██╗████╗  ██║╚══███╔╝██╔════╝
# ██████╔╝██████╔╝██║   ██║██╔██╗ ██║  ███╔╝ █████╗
# ██╔══██╗██╔══██╗██║   ██║██║╚██╗██║ ███╔╝  ██╔══╝
# ██████╔╝██║  ██║╚██████╔╝██║ ╚████║███████╗███████╗
# BRONZE LAYER – Raw append-only ingestion
# ---------------------------------------------------------------------------

BRONZE_TRADING_DATA = DeltaTableDef(
    layer="bronze",
    table_name="bronze_trading_data",
    relative_path="bronze/trading_data",
    description="Raw daily OHLCV + foreign flow crawled from cophieu68. Append-only.",
    partition_by=["_year", "_month"],
    primary_keys=["symbol", "date"],
    enable_cdf=True,
    columns=[
        ColumnDef("symbol",        "StringType",    False, "Stock ticker symbol"),
        ColumnDef("date",          "StringType",    False, "Trade date string dd/mm/yyyy"),
        ColumnDef("close_price",   "DoubleType",    True,  "Closing price"),
        ColumnDef("volume",        "LongType",      True,  "Total volume"),
        ColumnDef("open_price",    "DoubleType",    True,  "Opening price"),
        ColumnDef("high_price",    "DoubleType",    True,  "Highest price"),
        ColumnDef("low_price",     "DoubleType",    True,  "Lowest price"),
        ColumnDef("foreign_buy",   "LongType",      True,  "Foreign buy volume"),
        ColumnDef("foreign_sell",  "LongType",      True,  "Foreign sell volume"),
        ColumnDef("foreign_value", "DoubleType",    True,  "Foreign net value"),
        # Audit columns (Subsystem 6)
        ColumnDef("_ingested_at",  "TimestampType", False, "Ingestion timestamp"),
        ColumnDef("_source",       "StringType",    True,  "Source system name"),
        ColumnDef("_pipeline_run_id", "StringType", True,  "ETL run ID for lineage"),
        ColumnDef("_year",         "IntegerType",   True,  "Partition year"),
        ColumnDef("_month",        "IntegerType",   True,  "Partition month"),
    ],
)

BRONZE_COMPANY_INFO = DeltaTableDef(
    layer="bronze",
    table_name="bronze_company_info",
    relative_path="bronze/company_info",
    description="Raw company profile JSON from cophieu68. Upsert by symbol.",
    partition_by=[],
    primary_keys=["symbol"],
    enable_cdf=True,
    columns=[
        ColumnDef("symbol",          "StringType",    False),
        ColumnDef("profile_json",    "StringType",    True,  "Raw JSON profile"),
        ColumnDef("_ingested_at",    "TimestampType", False),
        ColumnDef("_source",         "StringType",    True),
        ColumnDef("_pipeline_run_id","StringType",    True),
    ],
)

BRONZE_INDUSTRY_INFO = DeltaTableDef(
    layer="bronze",
    table_name="bronze_industry_info",
    relative_path="bronze/industry_info",
    description="Raw industry info (summary/financial/fund) from cophieu68.",
    partition_by=[],
    primary_keys=["industry_metric", "industry_key"],
    enable_cdf=True,
    columns=[
        ColumnDef("industry_metric",  "StringType",    False, "summary_info|financial_info|fund_info"),
        ColumnDef("industry_key",     "StringType",    False, "Composite key _code_name_url_"),
        ColumnDef("data_json",        "StringType",    True,  "Raw JSON row"),
        ColumnDef("_ingested_at",     "TimestampType", False),
        ColumnDef("_source",          "StringType",    True),
        ColumnDef("_pipeline_run_id", "StringType",    True),
    ],
)

BRONZE_INCOME_STATEMENT = DeltaTableDef(
    layer="bronze",
    table_name="bronze_income_statement",
    relative_path="bronze/income_statement",
    description="Raw income statement HTML tables from cophieu68.",
    partition_by=["report_type"],
    primary_keys=["symbol", "report_type"],
    enable_cdf=True,
    columns=[
        ColumnDef("symbol",           "StringType",    False),
        ColumnDef("report_type",      "StringType",    False, "quarter|year"),
        ColumnDef("data_json",        "StringType",    True,  "Serialized DataFrame rows"),
        ColumnDef("_ingested_at",     "TimestampType", False),
        ColumnDef("_source",          "StringType",    True),
        ColumnDef("_pipeline_run_id", "StringType",    True),
    ],
)

BRONZE_BALANCE_SHEET = DeltaTableDef(
    layer="bronze",
    table_name="bronze_balance_sheet",
    relative_path="bronze/balance_sheet",
    description="Raw balance sheet HTML tables from cophieu68.",
    partition_by=["report_type"],
    primary_keys=["symbol", "report_type"],
    enable_cdf=True,
    columns=[
        ColumnDef("symbol",           "StringType",    False),
        ColumnDef("report_type",      "StringType",    False, "quarter|year"),
        ColumnDef("data_json",        "StringType",    True),
        ColumnDef("_ingested_at",     "TimestampType", False),
        ColumnDef("_source",          "StringType",    True),
        ColumnDef("_pipeline_run_id", "StringType",    True),
    ],
)

BRONZE_FINANCIAL_INFO = DeltaTableDef(
    layer="bronze",
    table_name="bronze_financial_info",
    relative_path="bronze/financial_info",
    description="Raw financial ratios (PE, PB, ROE, ROA, ...) per symbol.",
    partition_by=[],
    primary_keys=["symbol"],
    enable_cdf=True,
    columns=[
        ColumnDef("symbol",           "StringType",    False),
        ColumnDef("data_json",        "StringType",    True),
        ColumnDef("_ingested_at",     "TimestampType", False),
        ColumnDef("_source",          "StringType",    True),
        ColumnDef("_pipeline_run_id", "StringType",    True),
    ],
)

BRONZE_MATCH_DETAILS = DeltaTableDef(
    layer="bronze",
    table_name="bronze_match_details",
    relative_path="bronze/match_details",
    description="Raw intraday match details per symbol.",
    partition_by=[],
    primary_keys=["symbol"],
    enable_cdf=True,
    columns=[
        ColumnDef("symbol",           "StringType",    False),
        ColumnDef("data_json",        "StringType",    True),
        ColumnDef("_ingested_at",     "TimestampType", False),
        ColumnDef("_source",          "StringType",    True),
        ColumnDef("_pipeline_run_id", "StringType",    True),
    ],
)

BRONZE_BUSINESS_PLAN = DeltaTableDef(
    layer="bronze",
    table_name="bronze_business_plan",
    relative_path="bronze/business_plan",
    description="Raw business plan (revenue/profit targets) per symbol.",
    partition_by=[],
    primary_keys=["symbol"],
    enable_cdf=True,
    columns=[
        ColumnDef("symbol",           "StringType",    False),
        ColumnDef("data_json",        "StringType",    True),
        ColumnDef("_ingested_at",     "TimestampType", False),
        ColumnDef("_source",          "StringType",    True),
        ColumnDef("_pipeline_run_id", "StringType",    True),
    ],
)

BRONZE_MARKET_LIST = DeltaTableDef(
    layer="bronze",
    table_name="bronze_market_list",
    relative_path="bronze/market_list",
    description="Raw list of stocks per market type (VNINDEX, HNX, UPCOM, VN30).",
    partition_by=[],
    primary_keys=["market_type"],
    enable_cdf=False,
    columns=[
        ColumnDef("market_type",      "StringType",    False),
        ColumnDef("symbols_json",     "StringType",    True,  "JSON array of symbols"),
        ColumnDef("_ingested_at",     "TimestampType", False),
        ColumnDef("_source",          "StringType",    True),
        ColumnDef("_pipeline_run_id", "StringType",    True),
    ],
)

BRONZE_INDUSTRY_SECTORS = DeltaTableDef(
    layer="bronze",
    table_name="bronze_industry_sectors",
    relative_path="bronze/industry_sectors",
    description="Raw company-to-industry mapping.",
    partition_by=[],
    primary_keys=["industry_code", "symbol"],
    enable_cdf=True,
    columns=[
        ColumnDef("industry_code",        "StringType",  False),
        ColumnDef("industry_name",        "StringType",  True),
        ColumnDef("symbol",               "StringType",  False),
        ColumnDef("company_name",         "StringType",  True),
        ColumnDef("close_price",          "DoubleType",  True),
        ColumnDef("increase_decrease",    "DoubleType",  True),
        ColumnDef("volumn24h",            "DoubleType",  True),
        ColumnDef("volumn52w",            "DoubleType",  True),
        ColumnDef("listed_volumn",        "DoubleType",  True),
        ColumnDef("market_capitalization","DoubleType",  True),
        ColumnDef("_ingested_at",         "TimestampType", False),
        ColumnDef("_source",              "StringType",  True),
        ColumnDef("_pipeline_run_id",     "StringType",  True),
    ],
)

# ---------------------------------------------------------------------------
# ███████╗██╗██╗    ██╗   ██╗███████╗██████╗
# ██╔════╝██║██║    ██║   ██║██╔════╝██╔══██╗
# ███████╗██║██║    ██║   ██║█████╗  ██████╔╝
# ╚════██║██║██║    ╚██╗ ██╔╝██╔══╝  ██╔══██╗
# ███████║██║███████╗╚████╔╝ ███████╗██║  ██║
# SILVER LAYER – Cleaned, typed, SCD2 dimensions + fact tables
# ---------------------------------------------------------------------------

SILVER_DIM_COMPANY = DeltaTableDef(
    layer="silver",
    table_name="silver_dim_company",
    relative_path="silver/dim_company",
    description="SCD Type 2 company dimension. One row per version of company profile.",
    partition_by=[],
    primary_keys=["company_key"],
    enable_cdf=True,
    columns=[
        # Surrogate key (Subsystem 10)
        ColumnDef("company_key",          "StringType",    False, "SHA256 surrogate key"),
        ColumnDef("symbol",               "StringType",    False, "Natural key"),
        ColumnDef("company_name",         "StringType",    True),
        ColumnDef("full_name",            "StringType",    True),
        ColumnDef("english_name",         "StringType",    True),
        ColumnDef("short_name",           "StringType",    True),
        ColumnDef("address",              "StringType",    True),
        ColumnDef("phone",                "StringType",    True),
        ColumnDef("fax",                  "StringType",    True),
        ColumnDef("website",              "StringType",    True),
        ColumnDef("email_address",        "StringType",    True),
        ColumnDef("established_date",     "StringType",    True),
        ColumnDef("listed_date",          "StringType",    True),
        ColumnDef("listed_volume_initial","StringType",    True),
        ColumnDef("listed_volume",        "StringType",    True),
        ColumnDef("circulating_volume",   "StringType",    True),
        ColumnDef("market_capitalization","StringType",    True),
        # SCD2 columns (Subsystem 9)
        ColumnDef("effective_date",       "DateType",      False, "SCD2 valid from"),
        ColumnDef("end_date",             "DateType",      True,  "SCD2 valid to (NULL = current)"),
        ColumnDef("is_current",           "BooleanType",   False, "True if latest version"),
        ColumnDef("_row_hash",            "StringType",    True,  "Hash of tracked columns for change detection"),
        # Audit
        ColumnDef("_ingested_at",         "TimestampType", False),
        ColumnDef("_pipeline_run_id",     "StringType",    True),
    ],
)

SILVER_DIM_INDUSTRY = DeltaTableDef(
    layer="silver",
    table_name="silver_dim_industry",
    relative_path="silver/dim_industry",
    description="SCD Type 2 industry dimension.",
    partition_by=[],
    primary_keys=["industry_sk"],
    enable_cdf=True,
    columns=[
        ColumnDef("industry_sk",          "StringType",    False, "Surrogate key"),
        ColumnDef("industry_code",        "StringType",    False, "Natural key e.g. ^nh"),
        ColumnDef("industry_name",        "StringType",    True),
        ColumnDef("industry_metric",      "StringType",    True,  "summary_info|financial_info|fund_info"),
        ColumnDef("industry_craw_url",    "StringType",    True),
        ColumnDef("effective_date",       "DateType",      False),
        ColumnDef("end_date",             "DateType",      True),
        ColumnDef("is_current",           "BooleanType",   False),
        ColumnDef("_row_hash",            "StringType",    True),
        ColumnDef("_ingested_at",         "TimestampType", False),
        ColumnDef("_pipeline_run_id",     "StringType",    True),
    ],
)

SILVER_DIM_MARKET_TYPE = DeltaTableDef(
    layer="silver",
    table_name="silver_dim_market_type",
    relative_path="silver/dim_market_type",
    description="Market type dimension (VNINDEX, HNX, UPCOM, VN30). SCD Type 1.",
    partition_by=[],
    primary_keys=["market_key"],
    enable_cdf=False,
    columns=[
        ColumnDef("market_key",    "StringType",    False),
        ColumnDef("market_type",   "StringType",    False),
        ColumnDef("market_name",   "StringType",    True),
        ColumnDef("description",   "StringType",    True),
        ColumnDef("update_time",   "TimestampType", True),
        ColumnDef("_ingested_at",  "TimestampType", False),
    ],
)

SILVER_FACT_TRADING_HISTORY = DeltaTableDef(
    layer="silver",
    table_name="silver_fact_trading_history",
    relative_path="silver/fact_trading_history",
    description=(
        "Daily OHLCV + foreign flow. Append-only with dedup by (symbol, trade_date). "
        "Supports full historical query and Power BI time-series analysis."
    ),
    partition_by=["year", "month"],
    zorder_by=["symbol", "trade_date"],
    primary_keys=["trade_key"],
    enable_cdf=True,
    columns=[
        ColumnDef("trade_key",     "StringType",    False, "SHA256(symbol+date)"),
        ColumnDef("symbol",        "StringType",    False),
        ColumnDef("trade_date",    "DateType",      False, "Parsed trade date"),
        ColumnDef("close_price",   "DoubleType",    True),
        ColumnDef("open_price",    "DoubleType",    True),
        ColumnDef("high_price",    "DoubleType",    True),
        ColumnDef("low_price",     "DoubleType",    True),
        ColumnDef("volume",        "LongType",      True),
        ColumnDef("foreign_buy",   "LongType",      True),
        ColumnDef("foreign_sell",  "LongType",      True),
        ColumnDef("foreign_net_value", "DoubleType",True),
        ColumnDef("year",          "IntegerType",   True,  "Partition year"),
        ColumnDef("month",         "IntegerType",   True,  "Partition month"),
        ColumnDef("_ingested_at",  "TimestampType", False),
        ColumnDef("_pipeline_run_id", "StringType", True),
    ],
)

SILVER_FACT_INCOME_STATEMENT = DeltaTableDef(
    layer="silver",
    table_name="silver_fact_income_statement",
    relative_path="silver/fact_income_statement",
    description="Normalized income statement metrics (annual + quarterly). One row per symbol+period+metric.",
    partition_by=["time_report_type", "year"],
    primary_keys=["income_key"],
    enable_cdf=True,
    columns=[
        ColumnDef("income_key",           "StringType",    False, "SHA256(symbol+report_type+year+period+metric_code)"),
        ColumnDef("symbol",               "StringType",    False),
        ColumnDef("time_report_type",     "StringType",    True,  "ANNUALLY|QUARTERLY"),
        ColumnDef("financial_report_type","StringType",    True),
        ColumnDef("year",                 "StringType",    True),
        ColumnDef("period",               "StringType",    True,  "Q1|Q2|Q3|Q4 or NULL"),
        ColumnDef("metric_code",          "StringType",    True),
        ColumnDef("metric_name_en",       "StringType",    True),
        ColumnDef("metric_group",         "StringType",    True),
        ColumnDef("metric_value",         "DecimalType",   True),
        ColumnDef("currency",             "StringType",    True),
        ColumnDef("unit",                 "StringType",    True),
        ColumnDef("update_time",          "TimestampType", True),
        ColumnDef("_ingested_at",         "TimestampType", False),
        ColumnDef("_pipeline_run_id",     "StringType",    True),
    ],
)

SILVER_FACT_BALANCE_SHEET = DeltaTableDef(
    layer="silver",
    table_name="silver_fact_balance_sheet",
    relative_path="silver/fact_balance_sheet",
    description="Normalized balance sheet metrics (annual + quarterly). One row per symbol+period+metric.",
    partition_by=["time_report_type", "year"],
    primary_keys=["balance_key"],
    enable_cdf=True,
    columns=[
        ColumnDef("balance_key",          "StringType",    False, "SHA256(symbol+report_type+year+period+metric_code)"),
        ColumnDef("symbol",               "StringType",    False),
        ColumnDef("time_report_type",     "StringType",    True),
        ColumnDef("financial_report_type","StringType",    True),
        ColumnDef("year",                 "StringType",    True),
        ColumnDef("period",               "StringType",    True),
        ColumnDef("metric_code",          "StringType",    True),
        ColumnDef("metric_name_en",       "StringType",    True),
        ColumnDef("metric_group",         "StringType",    True),
        ColumnDef("metric_value",         "DecimalType",   True),
        ColumnDef("currency",             "StringType",    True),
        ColumnDef("unit",                 "StringType",    True),
        ColumnDef("update_time",          "TimestampType", True),
        ColumnDef("_ingested_at",         "TimestampType", False),
        ColumnDef("_pipeline_run_id",     "StringType",    True),
    ],
)

SILVER_FACT_FINANCIAL_METRICS = DeltaTableDef(
    layer="silver",
    table_name="silver_fact_financial_metrics",
    relative_path="silver/fact_financial_metrics",
    description="Financial ratios snapshot per symbol (PE, PB, ROE, ROA, Beta, MarketCap, ...).",
    partition_by=[],
    primary_keys=["financial_ratio_key"],
    enable_cdf=True,
    columns=[
        ColumnDef("financial_ratio_key",  "StringType",    False),
        ColumnDef("symbol",               "StringType",    False),
        ColumnDef("reference_price",      "DoubleType",    True),
        ColumnDef("open_price",           "DoubleType",    True),
        ColumnDef("high_price",           "DoubleType",    True),
        ColumnDef("low_price",            "DoubleType",    True),
        ColumnDef("volume",               "DoubleType",    True),
        ColumnDef("book_value",           "StringType",    True),
        ColumnDef("eps",                  "StringType",    True),
        ColumnDef("pe",                   "StringType",    True),
        ColumnDef("pb",                   "StringType",    True),
        ColumnDef("roe",                  "StringType",    True),
        ColumnDef("roa",                  "StringType",    True),
        ColumnDef("beta",                 "StringType",    True),
        ColumnDef("market_cap",           "StringType",    True),
        ColumnDef("listed_volume",        "StringType",    True),
        ColumnDef("avg_volume_52w",       "StringType",    True),
        ColumnDef("high_low_52w",         "StringType",    True),
        ColumnDef("debt",                 "StringType",    True),
        ColumnDef("equity",               "StringType",    True),
        ColumnDef("debt_to_equity",       "StringType",    True),
        ColumnDef("equity_to_assets",     "StringType",    True),
        ColumnDef("cash",                 "StringType",    True),
        ColumnDef("update_time",          "TimestampType", True),
        ColumnDef("_ingested_at",         "TimestampType", False),
        ColumnDef("_pipeline_run_id",     "StringType",    True),
    ],
)

SILVER_FACT_BUSINESS_PLAN = DeltaTableDef(
    layer="silver",
    table_name="silver_fact_business_plan",
    relative_path="silver/fact_business_plan",
    description="Business plan (revenue/profit targets) per symbol and year.",
    partition_by=[],
    primary_keys=["plan_key"],
    enable_cdf=False,
    columns=[
        ColumnDef("plan_key",        "StringType",    False, "SHA256(symbol+year)"),
        ColumnDef("symbol",          "StringType",    False),
        ColumnDef("year",            "StringType",    True),
        ColumnDef("plan_revenue",    "DoubleType",    True),
        ColumnDef("pass_revenue",    "DoubleType",    True),
        ColumnDef("plan_profit",     "DoubleType",    True),
        ColumnDef("pass_profit",     "DoubleType",    True),
        ColumnDef("update_time",     "TimestampType", True),
        ColumnDef("_ingested_at",    "TimestampType", False),
        ColumnDef("_pipeline_run_id","StringType",    True),
    ],
)

SILVER_FACT_INDUSTRY_SUMMARY = DeltaTableDef(
    layer="silver",
    table_name="silver_fact_industry_summary",
    relative_path="silver/fact_industry_summary",
    description="Industry-level aggregated metrics (index, PE, ROE, ROA, capital, ...).",
    partition_by=[],
    primary_keys=["industry_summary_key"],
    enable_cdf=True,
    columns=[
        ColumnDef("industry_summary_key",     "StringType",    False),
        ColumnDef("industry_code",            "StringType",    False),
        ColumnDef("industry_name",            "StringType",    True),
        ColumnDef("industry_metric_type",     "StringType",    True,  "summary_info|financial_info|fund_info"),
        ColumnDef("industry_index",           "DoubleType",    True),
        ColumnDef("percentage_change",        "DoubleType",    True),
        ColumnDef("liquidity",                "DoubleType",    True),
        ColumnDef("total_capital",            "DoubleType",    True),
        ColumnDef("average_price",            "StringType",    True),
        ColumnDef("book_value",               "StringType",    True),
        ColumnDef("eps",                      "StringType",    True),
        ColumnDef("pe",                       "DoubleType",    True),
        ColumnDef("roa",                      "DoubleType",    True),
        ColumnDef("roe",                      "DoubleType",    True),
        ColumnDef("supply_volumn",            "DoubleType",    True),
        ColumnDef("total_asset",              "DoubleType",    True),
        ColumnDef("total_equity",             "DoubleType",    True),
        ColumnDef("total_liabilities",        "DoubleType",    True),
        ColumnDef("percentage_debt_on_equity","DoubleType",    True),
        ColumnDef("percentage_equity_on_assets","DoubleType",  True),
        ColumnDef("revenue",                  "DoubleType",    True),
        ColumnDef("profit_before_tax",        "DoubleType",    True),
        ColumnDef("update_time",              "TimestampType", True),
        ColumnDef("_ingested_at",             "TimestampType", False),
        ColumnDef("_pipeline_run_id",         "StringType",    True),
    ],
)

# ---------------------------------------------------------------------------
#  ██████╗  ██████╗ ██╗     ██████╗
# ██╔════╝ ██╔═══██╗██║     ██╔══██╗
# ██║  ███╗██║   ██║██║     ██║  ██║
# ██║   ██║██║   ██║██║     ██║  ██║
# ╚██████╔╝╚██████╔╝███████╗██████╔╝
# GOLD LAYER – Business-ready, Power BI optimized
# ---------------------------------------------------------------------------

GOLD_FCT_TRADING_OHLCV = DeltaTableDef(
    layer="gold",
    table_name="gold_fct_trading_ohlcv",
    relative_path="gold/fct_trading_ohlcv",
    description=(
        "Optimized daily OHLCV for Power BI. ZORDER BY (symbol, trade_date). "
        "Includes company_name for direct BI consumption without joins."
    ),
    partition_by=["year", "month"],
    zorder_by=["symbol", "trade_date"],
    primary_keys=["trade_key"],
    enable_cdf=False,
    columns=[
        ColumnDef("trade_key",       "StringType",    False),
        ColumnDef("symbol",          "StringType",    False),
        ColumnDef("company_name",    "StringType",    True,  "Denormalized from dim_company"),
        ColumnDef("industry_name",   "StringType",    True,  "Denormalized from dim_industry"),
        ColumnDef("market_type",     "StringType",    True,  "Denormalized from dim_market_type"),
        ColumnDef("trade_date",      "DateType",      False),
        ColumnDef("close_price",     "DoubleType",    True),
        ColumnDef("open_price",      "DoubleType",    True),
        ColumnDef("high_price",      "DoubleType",    True),
        ColumnDef("low_price",       "DoubleType",    True),
        ColumnDef("volume",          "LongType",      True),
        ColumnDef("foreign_buy",     "LongType",      True),
        ColumnDef("foreign_sell",    "LongType",      True),
        ColumnDef("foreign_net_value","DoubleType",   True),
        ColumnDef("year",            "IntegerType",   True),
        ColumnDef("month",           "IntegerType",   True),
        ColumnDef("_updated_at",     "TimestampType", False),
    ],
)

GOLD_FCT_BS_ASSETS = DeltaTableDef(
    layer="gold",
    table_name="gold_fct_bs_assets",
    relative_path="gold/fct_bs_assets",
    description="Pivoted balance sheet assets (mirrors DBT mart fct_bs_assets). Power BI ready.",
    partition_by=["time_report_type"],
    zorder_by=["symbol", "year"],
    primary_keys=["symbol", "time_report_type", "year", "period"],
    enable_cdf=False,
    columns=[
        ColumnDef("symbol",                                    "StringType",  False),
        ColumnDef("time_report_type",                          "StringType",  True),
        ColumnDef("financial_report_type",                     "StringType",  True),
        ColumnDef("year",                                      "StringType",  True),
        ColumnDef("period",                                    "StringType",  True),
        ColumnDef("cash_and_valuables",                        "DecimalType", True),
        ColumnDef("deposits_with_central_bank",                "DecimalType", True),
        ColumnDef("t_bills_and_eligible_short_term_securities","DecimalType", True),
        ColumnDef("interbank_placements_and_loans",            "DecimalType", True),
        ColumnDef("trading_securities_net",                    "DecimalType", True),
        ColumnDef("derivatives_and_other_financial_assets",    "DecimalType", True),
        ColumnDef("loans_to_customers_net",                    "DecimalType", True),
        ColumnDef("investment_securities_net",                 "DecimalType", True),
        ColumnDef("long_term_investments_net",                 "DecimalType", True),
        ColumnDef("fixed_assets_net",                          "DecimalType", True),
        ColumnDef("investment_properties_net",                 "DecimalType", True),
        ColumnDef("other_assets_net",                          "DecimalType", True),
        ColumnDef("total_assets",                              "DecimalType", True),
        ColumnDef("update_time",                               "TimestampType", True),
        ColumnDef("_updated_at",                               "TimestampType", False),
    ],
)

GOLD_FCT_IS_CORE_INCOME = DeltaTableDef(
    layer="gold",
    table_name="gold_fct_is_core_income",
    relative_path="gold/fct_is_core_income",
    description="Pivoted income statement core income/expense. Power BI ready.",
    partition_by=["time_report_type"],
    zorder_by=["symbol", "year"],
    primary_keys=["symbol", "time_report_type", "year", "period"],
    enable_cdf=False,
    columns=[
        ColumnDef("symbol",                          "StringType",  False),
        ColumnDef("time_report_type",                "StringType",  True),
        ColumnDef("financial_report_type",           "StringType",  True),
        ColumnDef("year",                            "StringType",  True),
        ColumnDef("period",                          "StringType",  True),
        ColumnDef("net_interest_income",             "DecimalType", True),
        ColumnDef("interest_and_similar_income",     "DecimalType", True),
        ColumnDef("interest_and_similar_expenses",   "DecimalType", True),
        ColumnDef("net_fee_commission_income",       "DecimalType", True),
        ColumnDef("fee_commission_income",           "DecimalType", True),
        ColumnDef("fee_commission_expenses",         "DecimalType", True),
        ColumnDef("profit_before_tax",               "DecimalType", True),
        ColumnDef("profit_after_tax",                "DecimalType", True),
        ColumnDef("net_profit_attributable_to_parent","DecimalType",True),
        ColumnDef("update_time",                     "TimestampType", True),
        ColumnDef("_updated_at",                     "TimestampType", False),
    ],
)

GOLD_DIM_COMPANY_CURRENT = DeltaTableDef(
    layer="gold",
    table_name="gold_dim_company_current",
    relative_path="gold/dim_company_current",
    description="Current snapshot of dim_company (is_current=True). Optimized for Power BI slicers.",
    partition_by=[],
    zorder_by=["symbol"],
    primary_keys=["company_key"],
    enable_cdf=False,
    columns=[
        ColumnDef("company_key",          "StringType",    False),
        ColumnDef("symbol",               "StringType",    False),
        ColumnDef("company_name",         "StringType",    True),
        ColumnDef("full_name",            "StringType",    True),
        ColumnDef("english_name",         "StringType",    True),
        ColumnDef("short_name",           "StringType",    True),
        ColumnDef("address",              "StringType",    True),
        ColumnDef("website",              "StringType",    True),
        ColumnDef("established_date",     "StringType",    True),
        ColumnDef("listed_date",          "StringType",    True),
        ColumnDef("listed_volume",        "StringType",    True),
        ColumnDef("market_capitalization","StringType",    True),
        ColumnDef("effective_date",       "DateType",      True),
        ColumnDef("_updated_at",          "TimestampType", False),
    ],
)

# ---------------------------------------------------------------------------
# METADATA TABLES (Subsystem 34)
# ---------------------------------------------------------------------------

SILVER_META_ETL_RUN = DeltaTableDef(
    layer="silver",
    table_name="silver_meta_etl_run",
    relative_path="silver/meta/etl_run",
    description="ETL pipeline run log. Subsystem 22 (Job Scheduler) + 34 (Metadata Repository).",
    partition_by=[],
    primary_keys=["run_id"],
    enable_cdf=False,
    columns=[
        ColumnDef("run_id",        "StringType",    False, "UUID"),
        ColumnDef("job_name",      "StringType",    True),
        ColumnDef("layer",         "StringType",    True,  "bronze|silver|gold"),
        ColumnDef("table_name",    "StringType",    True),
        ColumnDef("start_time",    "TimestampType", True),
        ColumnDef("end_time",      "TimestampType", True),
        ColumnDef("status",        "StringType",    True,  "SUCCESS|FAILED|RUNNING"),
        ColumnDef("rows_read",     "LongType",      True),
        ColumnDef("rows_written",  "LongType",      True),
        ColumnDef("error_message", "StringType",    True),
    ],
)

SILVER_META_ETL_ERROR = DeltaTableDef(
    layer="silver",
    table_name="silver_meta_etl_error",
    relative_path="silver/meta/etl_error",
    description="Error event log. Subsystem 5 (Error Event Schema) + 30 (Problem Escalation).",
    partition_by=[],
    primary_keys=["err_id"],
    enable_cdf=False,
    columns=[
        ColumnDef("err_id",        "StringType",    False),
        ColumnDef("run_id",        "StringType",    True),
        ColumnDef("job_name",      "StringType",    True),
        ColumnDef("error_level",   "StringType",    True,  "WARNING|ERROR|FATAL"),
        ColumnDef("error_message", "StringType",    True),
        ColumnDef("record_id",     "StringType",    True,  "Natural key of failed record"),
        ColumnDef("raw_json",      "StringType",    True),
        ColumnDef("err_time",      "TimestampType", False),
        ColumnDef("retry_count",   "IntegerType",   True),
    ],
)

SILVER_META_DATA_QUALITY = DeltaTableDef(
    layer="silver",
    table_name="silver_meta_data_quality",
    relative_path="silver/meta/data_quality",
    description="Data quality check results. Subsystem 1 (Data Profiling).",
    partition_by=[],
    primary_keys=[],
    enable_cdf=False,
    columns=[
        ColumnDef("check_id",      "StringType",    False),
        ColumnDef("run_id",        "StringType",    True),
        ColumnDef("table_name",    "StringType",    True),
        ColumnDef("check_name",    "StringType",    True),
        ColumnDef("status",        "StringType",    True,  "PASS|FAIL|WARN"),
        ColumnDef("failed_rows",   "LongType",      True),
        ColumnDef("total_rows",    "LongType",      True),
        ColumnDef("checked_at",    "TimestampType", False),
        ColumnDef("details",       "StringType",    True),
    ],
)

# ---------------------------------------------------------------------------
# REGISTRY – single source of truth for all Delta tables
# ---------------------------------------------------------------------------

ALL_BRONZE_TABLES: Dict[str, DeltaTableDef] = {
    "trading_data":      BRONZE_TRADING_DATA,
    "company_info":      BRONZE_COMPANY_INFO,
    "industry_info":     BRONZE_INDUSTRY_INFO,
    "income_statement":  BRONZE_INCOME_STATEMENT,
    "balance_sheet":     BRONZE_BALANCE_SHEET,
    "financial_info":    BRONZE_FINANCIAL_INFO,
    "match_details":     BRONZE_MATCH_DETAILS,
    "business_plan":     BRONZE_BUSINESS_PLAN,
    "market_list":       BRONZE_MARKET_LIST,
    "industry_sectors":  BRONZE_INDUSTRY_SECTORS,
}

ALL_SILVER_TABLES: Dict[str, DeltaTableDef] = {
    "dim_company":            SILVER_DIM_COMPANY,
    "dim_industry":           SILVER_DIM_INDUSTRY,
    "dim_market_type":        SILVER_DIM_MARKET_TYPE,
    "fact_trading_history":   SILVER_FACT_TRADING_HISTORY,
    "fact_income_statement":  SILVER_FACT_INCOME_STATEMENT,
    "fact_balance_sheet":     SILVER_FACT_BALANCE_SHEET,
    "fact_financial_metrics": SILVER_FACT_FINANCIAL_METRICS,
    "fact_business_plan":     SILVER_FACT_BUSINESS_PLAN,
    "fact_industry_summary":  SILVER_FACT_INDUSTRY_SUMMARY,
    "meta_etl_run":           SILVER_META_ETL_RUN,
    "meta_etl_error":         SILVER_META_ETL_ERROR,
    "meta_data_quality":      SILVER_META_DATA_QUALITY,
}

ALL_GOLD_TABLES: Dict[str, DeltaTableDef] = {
    "fct_trading_ohlcv":    GOLD_FCT_TRADING_OHLCV,
    "fct_bs_assets":        GOLD_FCT_BS_ASSETS,
    "fct_is_core_income":   GOLD_FCT_IS_CORE_INCOME,
    "dim_company_current":  GOLD_DIM_COMPANY_CURRENT,
}


