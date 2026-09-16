"""
schema_registry.py — Data Modelling: Single Source of Truth
=============================================================
Thay thế delta_schema_registry.py (PySpark + Delta DDL).
Stack thực tế:
  - Polars dtypes  → enforce schema khi write Parquet (bronze/silver)
  - PostgreSQL DDL → CREATE TABLE cho staging + normalized tables

Kiến trúc 3 layer:
  BRONZE  → MinIO s3://lakehouse/bronze/<table>/   (Parquet, all-string, raw)
  SILVER  → MinIO s3://lakehouse/silver/<table>/   (Parquet, typed, deduplicated)
  GOLD    → PostgreSQL normalized tables

Nguyên tắc SOLID áp dụng:
  SRP  — mỗi TableDef chỉ mô tả schema, không chứa logic
  OCP  — thêm bảng mới = thêm instance, không sửa code cũ
  LSP  — tất cả TableDef dùng chung interface (to_polars_schema, to_pg_ddl)
  ISP  — tách ColumnDef.polars_dtype (Parquet) vs ColumnDef.pg_type (PostgreSQL)
  DIP  — bronze.py / serving.py depend on TableDef abstraction, không hardcode

Usage:
    from flows.cophieu68_deploy_full_pipeline.schema import get_bronze_table, get_silver_table

    tbl = get_bronze_table("trading_data")
    schema = tbl.to_polars_schema()          # Dict[str, pl.DataType]
    ddl    = tbl.to_pg_ddl("staging")        # CREATE TABLE IF NOT EXISTS …
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# ColumnDef — column descriptor cho cả Polars (Parquet) và PostgreSQL
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ColumnDef:
    """
    Mô tả một cột trong table definition.

    Attributes:
        name:          Tên cột (snake_case)
        polars_dtype:  Polars type string — dùng khi enforce Parquet schema.
                       Xem: https://docs.pola.rs/api/python/stable/reference/datatypes.html
                       Ví dụ: "Utf8", "Float64", "Int64", "Date", "Boolean"
        pg_type:       PostgreSQL column type cho CREATE TABLE.
                       Ví dụ: "TEXT", "NUMERIC(15,2)", "BIGINT", "DATE"
        nullable:      Có cho phép NULL không (default True).
                       Staging tables luôn nullable; normalized tables có thể NOT NULL.
        primary_key:   Đánh dấu PK cho normalized DDL.
        description:   Chú thích nghiệp vụ (ghi vào COMMENT ON COLUMN).
    """
    name:         str
    polars_dtype: str        # "Utf8" | "Float64" | "Int64" | "Date" | "Boolean" | …
    pg_type:      str        # "TEXT" | "NUMERIC(15,2)" | "BIGINT" | "DATE" | …
    nullable:     bool = True
    primary_key:  bool = False
    description:  str  = ""


# ---------------------------------------------------------------------------
# TableDef — table descriptor
# ---------------------------------------------------------------------------

@dataclass
class TableDef:
    """
    Mô tả một table trong data lakehouse.

    Attributes:
        layer:        "bronze" | "silver" | "gold"
        table_name:   Tên table (snake_case, không có prefix layer)
        columns:      Danh sách ColumnDef, thứ tự quan trọng
        partition_by: Tên cột dùng partition khi write Parquet
        primary_keys: Danh sách cột tạo nên PK (dùng cho ON CONFLICT trong PG)
        description:  Chú thích nghiệp vụ của table
    """
    layer:        str
    table_name:   str
    columns:      List[ColumnDef]
    partition_by: List[str]  = field(default_factory=list)
    primary_keys: List[str]  = field(default_factory=list)
    description:  str        = ""

    # ------------------------------------------------------------------
    # Polars integration
    # ------------------------------------------------------------------

    def to_polars_schema(self) -> Dict[str, object]:
        """
        Trả về Dict[col_name → pl.DataType] để dùng với:
            pl.DataFrame(records, schema_overrides=tbl.to_polars_schema())

        Bronze tables: tất cả cột là Utf8 (raw strings).
        Silver/Gold tables: dùng polars_dtype đã khai báo.
        """
        import polars as pl

        _TYPE_MAP: Dict[str, object] = {
            "Utf8":      pl.Utf8,
            "String":    pl.Utf8,
            "Int8":      pl.Int8,
            "Int16":     pl.Int16,
            "Int32":     pl.Int32,
            "Int64":     pl.Int64,
            "UInt8":     pl.UInt8,
            "UInt16":    pl.UInt16,
            "UInt32":    pl.UInt32,
            "UInt64":    pl.UInt64,
            "Float32":   pl.Float32,
            "Float64":   pl.Float64,
            "Boolean":   pl.Boolean,
            "Date":      pl.Date,
            "Datetime":  pl.Datetime,
        }

        if self.layer == "bronze":
            # Bronze = all strings — không ép type khi ingest
            return {col.name: pl.Utf8 for col in self.columns}

        return {
            col.name: _TYPE_MAP.get(col.polars_dtype, pl.Utf8)
            for col in self.columns
        }

    def column_names(self) -> List[str]:
        """Danh sách tên cột theo thứ tự khai báo."""
        return [col.name for col in self.columns]

    # ------------------------------------------------------------------
    # PostgreSQL DDL generation
    # ------------------------------------------------------------------

    def to_pg_ddl(
        self,
        schema: str = "public",
        *,
        include_audit_cols: bool = True,
    ) -> str:
        """
        Sinh ra CREATE TABLE IF NOT EXISTS SQL cho PostgreSQL.

        Args:
            schema:             Tên PG schema (staging | normalized | public)
            include_audit_cols: Thêm _ingested_at, _pipeline_run_id nếu chưa có

        Returns:
            SQL string hoàn chỉnh (không có trailing semicolon trên FROM/WHERE).
        """
        lines: List[str] = []
        for col in self.columns:
            null_clause = "" if col.nullable else " NOT NULL"
            pk_clause   = " PRIMARY KEY" if col.primary_key and len(self.primary_keys) == 1 else ""
            lines.append(f"    {col.name} {col.pg_type}{null_clause}{pk_clause}")

        # Audit columns (nếu chưa khai báo trong columns)
        existing_names = {c.name for c in self.columns}
        if include_audit_cols:
            if "_ingested_at" not in existing_names:
                lines.append("    _ingested_at TIMESTAMPTZ DEFAULT NOW()")
            if "_pipeline_run_id" not in existing_names:
                lines.append("    _pipeline_run_id TEXT")

        # Composite PK constraint
        if len(self.primary_keys) > 1:
            pk_cols = ", ".join(self.primary_keys)
            lines.append(f"    CONSTRAINT pk_{self.table_name} PRIMARY KEY ({pk_cols})")

        body = ",\n".join(lines)
        qualified = f"{schema}.{self.table_name}"
        return (
            f"-- {self.description}\n"
            f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
            f"{body}\n"
            f");"
        )

    def to_staging_ddl(self, staging_schema: str = "staging") -> str:
        """
        Sinh DDL cho staging table: tất cả cột là TEXT (raw load từ Parquet).
        Không có PK constraint trên staging.
        """
        lines = [f"    {col.name} TEXT" for col in self.columns]
        existing_names = {c.name for c in self.columns}
        if "_ingested_at" not in existing_names:
            lines.append("    _ingested_at TEXT")
        if "_pipeline_run_id" not in existing_names:
            lines.append("    _pipeline_run_id TEXT")

        body = ",\n".join(lines)
        qualified = f"{staging_schema}.{self.table_name}"
        return (
            f"-- Staging: {self.description}\n"
            f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
            f"{body}\n"
            f");"
        )


# ===========================================================================
# ══════════════════════════  BRONZE TABLES  ═════════════════════════════════
# Bronze = raw ingest từ cophieu68.vn, tất cả cột TEXT
# Polars schema sẽ luôn là all-Utf8 (do layer="bronze")
# ===========================================================================

_AUDIT_BRONZE: List[ColumnDef] = [
    ColumnDef("_batch_id",          "Utf8", "TEXT",        description="Batch ID ingest"),
    ColumnDef("_run_id",            "Utf8", "TEXT",        description="Pipeline run ID"),
    ColumnDef("_ingest_timestamp",  "Utf8", "TEXT",        description="ISO timestamp lúc ingest"),
    ColumnDef("ingest_date",        "Utf8", "TEXT",        description="Partition key YYYY-MM-DD"),
    ColumnDef("_dq_status",         "Utf8", "TEXT",        description="PASS | WARN"),
    ColumnDef("_dq_errors",         "Utf8", "TEXT",        description="DQ error messages"),
]

BRONZE_TRADING_DATA = TableDef(
    layer="bronze", table_name="stock_prices",
    description="Dữ liệu giao dịch lịch sử cổ phiếu (raw từ cophieu68)",
    partition_by=["ingest_date"],
    primary_keys=[],
    columns=[
        ColumnDef("symbol",        "Utf8", "TEXT", description="Mã cổ phiếu"),
        ColumnDef("date",          "Utf8", "TEXT", description="Ngày giao dịch"),
        ColumnDef("close_price",   "Utf8", "TEXT", description="Giá đóng cửa"),
        ColumnDef("volume",        "Utf8", "TEXT", description="Khối lượng giao dịch"),
        ColumnDef("open_price",    "Utf8", "TEXT", description="Giá mở cửa"),
        ColumnDef("high_price",    "Utf8", "TEXT", description="Giá cao nhất"),
        ColumnDef("low_price",     "Utf8", "TEXT", description="Giá thấp nhất"),
        ColumnDef("foreign_buy",   "Utf8", "TEXT", description="Khối lượng nước ngoài mua"),
        ColumnDef("foreign_sell",  "Utf8", "TEXT", description="Khối lượng nước ngoài bán"),
        ColumnDef("foreign_value", "Utf8", "TEXT", description="Giá trị giao dịch nước ngoài"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_COMPANY_PROFILE = TableDef(
    layer="bronze", table_name="company_profile",
    description="Thông tin hồ sơ công ty (raw)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("symbol",                "Utf8", "TEXT"),
        ColumnDef("current_price",         "Utf8", "TEXT"),
        ColumnDef("full_name",             "Utf8", "TEXT"),
        ColumnDef("english_name",          "Utf8", "TEXT"),
        ColumnDef("short_name",            "Utf8", "TEXT"),
        ColumnDef("address",               "Utf8", "TEXT"),
        ColumnDef("phone_number",          "Utf8", "TEXT"),
        ColumnDef("fax",                   "Utf8", "TEXT"),
        ColumnDef("website",               "Utf8", "TEXT"),
        ColumnDef("email_address",         "Utf8", "TEXT"),
        ColumnDef("established_date",      "Utf8", "TEXT"),
        ColumnDef("listed_date",           "Utf8", "TEXT"),
        ColumnDef("listed_volume_initial", "Utf8", "TEXT"),
        ColumnDef("listed_volume",         "Utf8", "TEXT"),
        ColumnDef("circulating_volume",    "Utf8", "TEXT"),
        ColumnDef("market_capitalization", "Utf8", "TEXT"),
        ColumnDef("foreign_buy",           "Utf8", "TEXT"),
        ColumnDef("foreign_ownership",     "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_FINANCIAL_RATIOS = TableDef(
    layer="bronze", table_name="financial_ratios",
    description="Chỉ số tài chính tóm tắt (raw)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("symbol",           "Utf8", "TEXT"),
        ColumnDef("reference_price",  "Utf8", "TEXT"),
        ColumnDef("open_price",       "Utf8", "TEXT"),
        ColumnDef("high_price",       "Utf8", "TEXT"),
        ColumnDef("low_price",        "Utf8", "TEXT"),
        ColumnDef("volume",           "Utf8", "TEXT"),
        ColumnDef("book_value",       "Utf8", "TEXT"),
        ColumnDef("eps",              "Utf8", "TEXT"),
        ColumnDef("pe",               "Utf8", "TEXT"),
        ColumnDef("pb",               "Utf8", "TEXT"),
        ColumnDef("roe",              "Utf8", "TEXT"),
        ColumnDef("roa",              "Utf8", "TEXT"),
        ColumnDef("beta",             "Utf8", "TEXT"),
        ColumnDef("market_cap",       "Utf8", "TEXT"),
        ColumnDef("listed_volume",    "Utf8", "TEXT"),
        ColumnDef("avg_volume_52w",   "Utf8", "TEXT"),
        ColumnDef("high_low_52w",     "Utf8", "TEXT"),
        ColumnDef("debt",             "Utf8", "TEXT"),
        ColumnDef("equity",           "Utf8", "TEXT"),
        ColumnDef("debt_to_equity",   "Utf8", "TEXT"),
        ColumnDef("equity_to_assets", "Utf8", "TEXT"),
        ColumnDef("cash",             "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_INCOME_STATEMENT = TableDef(
    layer="bronze", table_name="income_statement",
    description="Báo cáo kết quả kinh doanh (raw, quarter + year merged)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("symbol",      "Utf8", "TEXT"),
        ColumnDef("report_type", "Utf8", "TEXT", description="quarter | year"),
        ColumnDef("period",      "Utf8", "TEXT"),
        ColumnDef("revenue",     "Utf8", "TEXT"),
        ColumnDef("gross_profit","Utf8", "TEXT"),
        ColumnDef("ebit",        "Utf8", "TEXT"),
        ColumnDef("ebt",         "Utf8", "TEXT"),
        ColumnDef("net_profit",  "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_BALANCE_SHEET = TableDef(
    layer="bronze", table_name="balance_sheet",
    description="Bảng cân đối kế toán (raw, quarter + year merged)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("symbol",           "Utf8", "TEXT"),
        ColumnDef("report_type",      "Utf8", "TEXT", description="quarter | year"),
        ColumnDef("period",           "Utf8", "TEXT"),
        ColumnDef("total_assets",     "Utf8", "TEXT"),
        ColumnDef("current_assets",   "Utf8", "TEXT"),
        ColumnDef("long_term_assets", "Utf8", "TEXT"),
        ColumnDef("total_liabilities","Utf8", "TEXT"),
        ColumnDef("short_term_debt",  "Utf8", "TEXT"),
        ColumnDef("long_term_debt",   "Utf8", "TEXT"),
        ColumnDef("equity",           "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_BUSINESS_PLAN = TableDef(
    layer="bronze", table_name="business_plan",
    description="Kế hoạch kinh doanh từng năm (raw)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("symbol",       "Utf8", "TEXT"),
        ColumnDef("Year",         "Utf8", "TEXT"),
        ColumnDef("Plan_revenue", "Utf8", "TEXT"),
        ColumnDef("Pass_revenue", "Utf8", "TEXT"),
        ColumnDef("Plan_profit",  "Utf8", "TEXT"),
        ColumnDef("Pass_profit",  "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_INDUSTRY_SECTORS = TableDef(
    layer="bronze", table_name="industry_sectors",
    description="Danh sách công ty thuộc nhóm ngành (raw)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("industry_code",         "Utf8", "TEXT"),
        ColumnDef("industry_name",         "Utf8", "TEXT"),
        ColumnDef("symbol",                "Utf8", "TEXT"),
        ColumnDef("company_name",          "Utf8", "TEXT"),
        ColumnDef("close_price",           "Utf8", "TEXT"),
        ColumnDef("Increase_decrease",     "Utf8", "TEXT"),
        ColumnDef("volumn24h",             "Utf8", "TEXT"),
        ColumnDef("volumn52w",             "Utf8", "TEXT"),
        ColumnDef("listed_volumn",         "Utf8", "TEXT"),
        ColumnDef("market_capitalization", "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_MARKET_TYPE_SECTORS = TableDef(
    layer="bronze", table_name="market_type_sectors",
    description="Danh sách công ty theo loại thị trường HOSE/HNX/UPCOM (raw)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("market_type_code",      "Utf8", "TEXT"),
        ColumnDef("market_type_name",      "Utf8", "TEXT"),
        ColumnDef("symbol",                "Utf8", "TEXT"),
        ColumnDef("company_name",          "Utf8", "TEXT"),
        ColumnDef("close_price",           "Utf8", "TEXT"),
        ColumnDef("Increase_decrease",     "Utf8", "TEXT"),
        ColumnDef("volumn24h",             "Utf8", "TEXT"),
        ColumnDef("volumn52w",             "Utf8", "TEXT"),
        ColumnDef("listed_volumn",         "Utf8", "TEXT"),
        ColumnDef("market_capitalization", "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_FINANCIAL_REPORT_SUMMARY = TableDef(
    layer="bronze", table_name="financial_report_summary",
    description="Báo cáo tài chính tóm tắt HTML tables (raw)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("symbol",      "Utf8", "TEXT"),
        ColumnDef("report_type", "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_INDUSTRY_INFO_SUMMARY = TableDef(
    layer="bronze", table_name="industry_info_summary_info",
    description="Thông tin tóm tắt ngành (raw)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("_industry_key", "Utf8", "TEXT"),
        ColumnDef("index",         "Utf8", "TEXT"),
        ColumnDef("change",        "Utf8", "TEXT"),
        ColumnDef("liquidity",     "Utf8", "TEXT"),
        ColumnDef("capital",       "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_INDUSTRY_INFO_FINANCIAL = TableDef(
    layer="bronze", table_name="industry_info_financial_info",
    description="Thông tin tài chính ngành (raw)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("_industry_key", "Utf8", "TEXT"),
        ColumnDef("avg_price",     "Utf8", "TEXT"),
        ColumnDef("book_value",    "Utf8", "TEXT"),
        ColumnDef("eps",           "Utf8", "TEXT"),
        ColumnDef("pe",            "Utf8", "TEXT"),
        ColumnDef("roa",           "Utf8", "TEXT"),
        ColumnDef("roe",           "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)

BRONZE_INDUSTRY_INFO_FUND = TableDef(
    layer="bronze", table_name="industry_info_fund_info",
    description="Thông tin vốn ngành (raw)",
    partition_by=["ingest_date"],
    columns=[
        ColumnDef("_industry_key",              "Utf8", "TEXT"),
        ColumnDef("supply_volumn",              "Utf8", "TEXT"),
        ColumnDef("total_asset",                "Utf8", "TEXT"),
        ColumnDef("total_equity",               "Utf8", "TEXT"),
        ColumnDef("total_liabilities",          "Utf8", "TEXT"),
        ColumnDef("percentage_debt_on_equity",  "Utf8", "TEXT"),
        ColumnDef("percentage_equity_on_assets","Utf8", "TEXT"),
        ColumnDef("revenue",                    "Utf8", "TEXT"),
        ColumnDef("profit_before_tax",          "Utf8", "TEXT"),
        *_AUDIT_BRONZE,
    ],
)


# ===========================================================================
# ══════════════════════════  SILVER TABLES  ═════════════════════════════════
# Silver = typed + deduplicated Parquet trên MinIO
# Silver tables are materialized directly by the Polars transforms.
# ===========================================================================

SILVER_FACT_STOCK_PRICE = TableDef(
    layer="silver", table_name="fact_stock_price",
    description="Fact bảng giá giao dịch lịch sử cổ phiếu (Silver)",
    partition_by=["year", "month"],
    primary_keys=["trade_key"],
    columns=[
        ColumnDef("trade_key",         "Utf8",    "TEXT",           primary_key=True),
        ColumnDef("symbol",            "Utf8",    "TEXT",           nullable=False),
        ColumnDef("date",              "Utf8",    "TEXT",           description="YYYY-MM-DD string"),
        ColumnDef("close_price",       "Float64", "NUMERIC(15,2)"),
        ColumnDef("open_price",        "Float64", "NUMERIC(15,2)"),
        ColumnDef("high_price",        "Float64", "NUMERIC(15,2)"),
        ColumnDef("low_price",         "Float64", "NUMERIC(15,2)"),
        ColumnDef("volume",            "Int64",   "BIGINT"),
        ColumnDef("foreign_buy",       "Int64",   "BIGINT"),
        ColumnDef("foreign_sell",      "Int64",   "BIGINT"),
        ColumnDef("foreign_value",     "Float64", "NUMERIC(20,2)"),
        ColumnDef("year",              "Int32",   "SMALLINT"),
        ColumnDef("month",             "Int32",   "SMALLINT"),
        ColumnDef("_ingested_at",      "Utf8",    "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id",  "Utf8",    "TEXT"),
    ],
)

SILVER_DIM_COMPANY = TableDef(
    layer="silver", table_name="dim_company",
    description="Dimension công ty (SCD Type 2)",
    partition_by=[],
    primary_keys=["company_key"],
    columns=[
        ColumnDef("company_key",           "Utf8",    "TEXT",       primary_key=True),
        ColumnDef("symbol",                "Utf8",    "TEXT",       nullable=False),
        ColumnDef("full_name",             "Utf8",    "TEXT"),
        ColumnDef("english_name",          "Utf8",    "TEXT"),
        ColumnDef("short_name",            "Utf8",    "TEXT"),
        ColumnDef("address",               "Utf8",    "TEXT"),
        ColumnDef("phone_number",          "Utf8",    "TEXT"),
        ColumnDef("fax",                   "Utf8",    "TEXT"),
        ColumnDef("website",               "Utf8",    "TEXT"),
        ColumnDef("email_address",         "Utf8",    "TEXT"),
        ColumnDef("established_date",      "Utf8",    "TEXT"),
        ColumnDef("listed_date",           "Utf8",    "TEXT"),
        ColumnDef("listed_volume_initial", "Utf8",    "TEXT"),
        ColumnDef("listed_volume",         "Utf8",    "TEXT"),
        ColumnDef("circulating_volume",    "Utf8",    "TEXT"),
        ColumnDef("market_capitalization", "Utf8",    "TEXT"),
        ColumnDef("foreign_buy",           "Utf8",    "TEXT"),
        ColumnDef("foreign_ownership",     "Utf8",    "TEXT"),
        ColumnDef("effective_date",        "Utf8",    "TEXT"),
        ColumnDef("_row_hash",             "Utf8",    "TEXT"),
        ColumnDef("_ingested_at",          "Utf8",    "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id",      "Utf8",    "TEXT"),
    ],
)

SILVER_FACT_FINANCIAL_METRICS = TableDef(
    layer="silver", table_name="fact_financial_metrics",
    description="Fact chỉ số tài chính mỗi mã cổ phiếu (Silver)",
    partition_by=[],
    primary_keys=["financial_ratio_key"],
    columns=[
        ColumnDef("financial_ratio_key", "Utf8",    "TEXT",          primary_key=True),
        ColumnDef("symbol",              "Utf8",    "TEXT",          nullable=False),
        ColumnDef("reference_price",     "Float64", "NUMERIC(15,2)"),
        ColumnDef("open_price",          "Float64", "NUMERIC(15,2)"),
        ColumnDef("high_price",          "Float64", "NUMERIC(15,2)"),
        ColumnDef("low_price",           "Float64", "NUMERIC(15,2)"),
        ColumnDef("volume",              "Int64",   "BIGINT"),
        ColumnDef("book_value",          "Float64", "NUMERIC(15,2)"),
        ColumnDef("eps",                 "Float64", "NUMERIC(15,4)"),
        ColumnDef("pe",                  "Float64", "NUMERIC(10,2)"),
        ColumnDef("pb",                  "Float64", "NUMERIC(10,2)"),
        ColumnDef("roe",                 "Float64", "NUMERIC(10,4)"),
        ColumnDef("roa",                 "Float64", "NUMERIC(10,4)"),
        ColumnDef("beta",                "Float64", "NUMERIC(10,4)"),
        ColumnDef("market_cap",          "Float64", "NUMERIC(25,2)"),
        ColumnDef("listed_volume",       "Int64",   "BIGINT"),
        ColumnDef("avg_volume_52w",      "Int64",   "BIGINT"),
        ColumnDef("debt",                "Float64", "NUMERIC(20,2)"),
        ColumnDef("equity",              "Float64", "NUMERIC(20,2)"),
        ColumnDef("debt_to_equity",      "Float64", "NUMERIC(10,4)"),
        ColumnDef("equity_to_assets",    "Float64", "NUMERIC(10,4)"),
        ColumnDef("cash",                "Float64", "NUMERIC(20,2)"),
        ColumnDef("_ingested_at",        "Utf8",    "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id",    "Utf8",    "TEXT"),
    ],
)

SILVER_FACT_BUSINESS_PLAN = TableDef(
    layer="silver", table_name="fact_business_plan",
    description="Fact kế hoạch kinh doanh theo năm (Silver)",
    partition_by=[],
    primary_keys=["plan_key"],
    columns=[
        ColumnDef("plan_key",      "Utf8",    "TEXT",         primary_key=True),
        ColumnDef("symbol",        "Utf8",    "TEXT",         nullable=False),
        ColumnDef("Year",          "Utf8",    "TEXT"),
        ColumnDef("Plan_revenue",  "Float64", "NUMERIC(20,2)"),
        ColumnDef("Pass_revenue",  "Float64", "NUMERIC(20,2)"),
        ColumnDef("Plan_profit",   "Float64", "NUMERIC(20,2)"),
        ColumnDef("Pass_profit",   "Float64", "NUMERIC(20,2)"),
        ColumnDef("_ingested_at",  "Utf8",    "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id", "Utf8", "TEXT"),
    ],
)

SILVER_FACT_INCOME_STATEMENT = TableDef(
    layer="silver", table_name="fact_income_statement",
    description="Fact báo cáo kết quả kinh doanh (Silver)",
    partition_by=[],
    primary_keys=["symbol", "report_type", "period"],
    columns=[
        ColumnDef("symbol",        "Utf8",    "TEXT", nullable=False),
        ColumnDef("report_type",   "Utf8",    "TEXT", nullable=False, description="quarter | year"),
        ColumnDef("period",        "Utf8",    "TEXT", nullable=False),
        ColumnDef("revenue",       "Float64", "NUMERIC(20,2)"),
        ColumnDef("gross_profit",  "Float64", "NUMERIC(20,2)"),
        ColumnDef("ebit",          "Float64", "NUMERIC(20,2)"),
        ColumnDef("ebt",           "Float64", "NUMERIC(20,2)"),
        ColumnDef("net_profit",    "Float64", "NUMERIC(20,2)"),
        ColumnDef("_ingested_at",  "Utf8",    "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id", "Utf8", "TEXT"),
    ],
)

SILVER_FACT_BALANCE_SHEET = TableDef(
    layer="silver", table_name="fact_balance_sheet",
    description="Fact bảng cân đối kế toán (Silver)",
    partition_by=[],
    primary_keys=["symbol", "report_type", "period"],
    columns=[
        ColumnDef("symbol",            "Utf8",    "TEXT", nullable=False),
        ColumnDef("report_type",       "Utf8",    "TEXT", nullable=False),
        ColumnDef("period",            "Utf8",    "TEXT", nullable=False),
        ColumnDef("total_assets",      "Float64", "NUMERIC(20,2)"),
        ColumnDef("current_assets",    "Float64", "NUMERIC(20,2)"),
        ColumnDef("long_term_assets",  "Float64", "NUMERIC(20,2)"),
        ColumnDef("total_liabilities", "Float64", "NUMERIC(20,2)"),
        ColumnDef("short_term_debt",   "Float64", "NUMERIC(20,2)"),
        ColumnDef("long_term_debt",    "Float64", "NUMERIC(20,2)"),
        ColumnDef("equity",            "Float64", "NUMERIC(20,2)"),
        ColumnDef("_ingested_at",      "Utf8",    "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id",  "Utf8",    "TEXT"),
    ],
)

SILVER_DIM_INDUSTRY = TableDef(
    layer="silver", table_name="dim_industry",
    description="Dimension ngành nghề (SCD Type 2)",
    partition_by=[],
    primary_keys=["industry_sk"],
    columns=[
        ColumnDef("industry_sk",        "Utf8", "TEXT", primary_key=True),
        ColumnDef("industry_code",      "Utf8", "TEXT", nullable=False),
        ColumnDef("industry_name",      "Utf8", "TEXT"),
        ColumnDef("effective_date",     "Utf8", "TEXT"),
        ColumnDef("_row_hash",          "Utf8", "TEXT"),
        ColumnDef("_ingested_at",       "Utf8", "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id",   "Utf8", "TEXT"),
    ],
)

SILVER_DIM_MARKET_TYPE = TableDef(
    layer="silver", table_name="dim_market_type",
    description="Dimension loại thị trường HOSE/HNX/UPCOM/VN30",
    partition_by=[],
    primary_keys=["market_key"],
    columns=[
        ColumnDef("market_key",        "Utf8", "TEXT", primary_key=True),
        ColumnDef("market_type_code",  "Utf8", "TEXT"),
        ColumnDef("market_type_name",  "Utf8", "TEXT"),
        ColumnDef("_ingested_at",      "Utf8", "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id",  "Utf8", "TEXT"),
    ],
)

SILVER_FACT_INDUSTRY_SUMMARY = TableDef(
    layer="silver", table_name="fact_industry_summary",
    description="Fact thống kê ngành: summary + financial + fund info (Silver)",
    partition_by=[],
    primary_keys=["_industry_key"],
    columns=[
        ColumnDef("_industry_key",              "Utf8",    "TEXT",         primary_key=True),
        # summary_info
        ColumnDef("index",                      "Utf8",    "TEXT"),
        ColumnDef("change",                     "Utf8",    "TEXT"),
        ColumnDef("liquidity",                  "Utf8",    "TEXT"),
        ColumnDef("capital",                    "Utf8",    "TEXT"),
        # financial_info
        ColumnDef("avg_price",                  "Float64", "NUMERIC(15,2)"),
        ColumnDef("book_value",                 "Float64", "NUMERIC(15,2)"),
        ColumnDef("eps",                        "Float64", "NUMERIC(15,4)"),
        ColumnDef("pe",                         "Float64", "NUMERIC(10,2)"),
        ColumnDef("roa",                        "Float64", "NUMERIC(10,4)"),
        ColumnDef("roe",                        "Float64", "NUMERIC(10,4)"),
        # fund_info
        ColumnDef("supply_volumn",              "Int64",   "BIGINT"),
        ColumnDef("total_asset",                "Float64", "NUMERIC(20,2)"),
        ColumnDef("total_equity",               "Float64", "NUMERIC(20,2)"),
        ColumnDef("total_liabilities",          "Float64", "NUMERIC(20,2)"),
        ColumnDef("percentage_debt_on_equity",  "Float64", "NUMERIC(10,4)"),
        ColumnDef("percentage_equity_on_assets","Float64", "NUMERIC(10,4)"),
        ColumnDef("revenue",                    "Float64", "NUMERIC(20,2)"),
        ColumnDef("profit_before_tax",          "Float64", "NUMERIC(20,2)"),
        ColumnDef("_ingested_at",               "Utf8",    "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id",           "Utf8",    "TEXT"),
    ],
)

SILVER_FACT_FINANCIAL_REPORT = TableDef(
    layer="silver", table_name="fact_financial_report",
    description="Fact báo cáo tài chính đầy đủ HTML table (Silver)",
    partition_by=[],
    primary_keys=["symbol", "report_type"],
    columns=[
        ColumnDef("symbol",               "Utf8", "TEXT", nullable=False),
        ColumnDef("report_type",          "Utf8", "TEXT"),
        ColumnDef("_ingested_at",         "Utf8", "TIMESTAMPTZ"),
        ColumnDef("_pipeline_run_id",     "Utf8", "TEXT"),
    ],
)


# ===========================================================================
# ════════════════════════════  GOLD TABLES  ══════════════════════════════════
# Gold = normalized PostgreSQL tables (serving layer)
# DDL dùng để CREATE TABLE trong PostgreSQL schema "normalized"
# ===========================================================================

GOLD_FACT_STOCK_PRICE = TableDef(
    layer="gold", table_name="fact_stock_price",
    description="Fact giá giao dịch cổ phiếu (normalized PostgreSQL)",
    primary_keys=["trade_key"],
    columns=[
        ColumnDef("trade_key",         "Utf8",    "TEXT",           primary_key=True, nullable=False),
        ColumnDef("symbol",            "Utf8",    "TEXT",           nullable=False),
        ColumnDef("trade_date",        "Date",    "DATE"),
        ColumnDef("close_price",       "Float64", "NUMERIC(15,2)"),
        ColumnDef("open_price",        "Float64", "NUMERIC(15,2)"),
        ColumnDef("high_price",        "Float64", "NUMERIC(15,2)"),
        ColumnDef("low_price",         "Float64", "NUMERIC(15,2)"),
        ColumnDef("volume",            "Int64",   "BIGINT"),
        ColumnDef("foreign_buy",       "Int64",   "BIGINT"),
        ColumnDef("foreign_sell",      "Int64",   "BIGINT"),
        ColumnDef("foreign_net_value", "Float64", "NUMERIC(20,2)"),
        ColumnDef("year",              "Int32",   "SMALLINT"),
        ColumnDef("month",             "Int32",   "SMALLINT"),
        ColumnDef("_ingested_at",      "Utf8",    "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_pipeline_run_id",  "Utf8",    "TEXT"),
    ],
)

GOLD_DIM_COMPANY = TableDef(
    layer="gold", table_name="dim_company",
    description="Dimension công ty (SCD Type 2, normalized PostgreSQL)",
    primary_keys=["company_key"],
    columns=[
        ColumnDef("company_key",           "Utf8",    "TEXT",       primary_key=True, nullable=False),
        ColumnDef("symbol",                "Utf8",    "TEXT",       nullable=False),
        ColumnDef("full_name",             "Utf8",    "TEXT"),
        ColumnDef("english_name",          "Utf8",    "TEXT"),
        ColumnDef("short_name",            "Utf8",    "TEXT"),
        ColumnDef("address",               "Utf8",    "TEXT"),
        ColumnDef("phone_number",          "Utf8",    "TEXT"),
        ColumnDef("fax",                   "Utf8",    "TEXT"),
        ColumnDef("website",               "Utf8",    "TEXT"),
        ColumnDef("email_address",         "Utf8",    "TEXT"),
        ColumnDef("established_date",      "Date",    "DATE"),
        ColumnDef("listed_date",           "Date",    "DATE"),
        ColumnDef("listed_volume_initial", "Int64",   "BIGINT"),
        ColumnDef("listed_volume",         "Int64",   "BIGINT"),
        ColumnDef("circulating_volume",    "Int64",   "BIGINT"),
        ColumnDef("market_capitalization", "Float64", "NUMERIC(20,2)"),
        ColumnDef("foreign_buy",           "Utf8",    "TEXT"),
        ColumnDef("foreign_ownership",     "Utf8",    "TEXT"),
        ColumnDef("effective_date",        "Date",    "DATE"),
        ColumnDef("end_date",              "Date",    "DATE"),
        ColumnDef("is_current",            "Boolean", "BOOLEAN DEFAULT TRUE"),
        ColumnDef("_row_hash",             "Utf8",    "TEXT"),
        ColumnDef("_ingested_at",          "Utf8",    "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_pipeline_run_id",      "Utf8",    "TEXT"),
    ],
)

GOLD_FACT_FINANCIAL_METRICS = TableDef(
    layer="gold", table_name="fact_financial_metrics",
    description="Fact chỉ số tài chính (normalized PostgreSQL)",
    primary_keys=["financial_ratio_key"],
    columns=[
        ColumnDef("financial_ratio_key", "Utf8",    "TEXT",          primary_key=True, nullable=False),
        ColumnDef("symbol",              "Utf8",    "TEXT",          nullable=False),
        ColumnDef("reference_price",     "Float64", "NUMERIC(15,2)"),
        ColumnDef("open_price",          "Float64", "NUMERIC(15,2)"),
        ColumnDef("high_price",          "Float64", "NUMERIC(15,2)"),
        ColumnDef("low_price",           "Float64", "NUMERIC(15,2)"),
        ColumnDef("volume",              "Int64",   "BIGINT"),
        ColumnDef("book_value",          "Float64", "NUMERIC(15,2)"),
        ColumnDef("eps",                 "Float64", "NUMERIC(15,4)"),
        ColumnDef("pe",                  "Float64", "NUMERIC(10,2)"),
        ColumnDef("pb",                  "Float64", "NUMERIC(10,2)"),
        ColumnDef("roe",                 "Float64", "NUMERIC(10,4)"),
        ColumnDef("roa",                 "Float64", "NUMERIC(10,4)"),
        ColumnDef("beta",                "Float64", "NUMERIC(10,4)"),
        ColumnDef("market_cap",          "Float64", "NUMERIC(25,2)"),
        ColumnDef("listed_volume",       "Int64",   "BIGINT"),
        ColumnDef("avg_volume_52w",      "Int64",   "BIGINT"),
        ColumnDef("debt",                "Float64", "NUMERIC(20,2)"),
        ColumnDef("equity",              "Float64", "NUMERIC(20,2)"),
        ColumnDef("debt_to_equity",      "Float64", "NUMERIC(10,4)"),
        ColumnDef("equity_to_assets",    "Float64", "NUMERIC(10,4)"),
        ColumnDef("cash",                "Float64", "NUMERIC(20,2)"),
        ColumnDef("update_time",         "Utf8",    "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_ingested_at",        "Utf8",    "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_pipeline_run_id",    "Utf8",    "TEXT"),
    ],
)

GOLD_FACT_BUSINESS_PLAN = TableDef(
    layer="gold", table_name="fact_business_plan",
    description="Fact kế hoạch kinh doanh (normalized PostgreSQL)",
    primary_keys=["plan_key"],
    columns=[
        ColumnDef("plan_key",                "Utf8",    "TEXT",          primary_key=True, nullable=False),
        ColumnDef("symbol",                  "Utf8",    "TEXT",          nullable=False),
        ColumnDef("year",                    "Int32",   "SMALLINT"),
        ColumnDef("plan_revenue",            "Float64", "NUMERIC(20,2)"),
        ColumnDef("pass_revenue",            "Float64", "NUMERIC(20,2)"),
        ColumnDef("plan_profit",             "Float64", "NUMERIC(20,2)"),
        ColumnDef("pass_profit",             "Float64", "NUMERIC(20,2)"),
        ColumnDef("revenue_completion_rate", "Float64", "NUMERIC(10,4)"),
        ColumnDef("profit_completion_rate",  "Float64", "NUMERIC(10,4)"),
        ColumnDef("update_time",             "Utf8",    "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_ingested_at",            "Utf8",    "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_pipeline_run_id",        "Utf8",    "TEXT"),
    ],
)

GOLD_DIM_INDUSTRY = TableDef(
    layer="gold", table_name="dim_industry",
    description="Dimension ngành nghề (SCD2, normalized PostgreSQL)",
    primary_keys=["industry_sk"],
    columns=[
        ColumnDef("industry_sk",        "Utf8", "TEXT", primary_key=True, nullable=False),
        ColumnDef("industry_code",      "Utf8", "TEXT", nullable=False),
        ColumnDef("industry_name",      "Utf8", "TEXT"),
        ColumnDef("effective_date",     "Date", "DATE"),
        ColumnDef("end_date",           "Date", "DATE"),
        ColumnDef("is_current",         "Boolean", "BOOLEAN DEFAULT TRUE"),
        ColumnDef("_row_hash",          "Utf8", "TEXT"),
        ColumnDef("_ingested_at",       "Utf8", "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_pipeline_run_id",   "Utf8", "TEXT"),
    ],
)

GOLD_DIM_MARKET_TYPE = TableDef(
    layer="gold", table_name="dim_market_type",
    description="Dimension loại thị trường (normalized PostgreSQL)",
    primary_keys=["market_key"],
    columns=[
        ColumnDef("market_key",       "Utf8", "TEXT", primary_key=True, nullable=False),
        ColumnDef("market_type",      "Utf8", "TEXT"),
        ColumnDef("market_name",      "Utf8", "TEXT"),
        ColumnDef("update_time",      "Utf8", "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_ingested_at",     "Utf8", "TIMESTAMPTZ DEFAULT NOW()"),
    ],
)

GOLD_FACT_INCOME_STATEMENT = TableDef(
    layer="gold", table_name="fact_income_statement",
    description="Fact báo cáo kết quả kinh doanh (normalized PostgreSQL)",
    primary_keys=["symbol", "report_type", "period"],
    columns=[
        ColumnDef("symbol",        "Utf8",    "TEXT",          nullable=False),
        ColumnDef("report_type",   "Utf8",    "TEXT",          nullable=False),
        ColumnDef("period",        "Utf8",    "TEXT",          nullable=False),
        ColumnDef("revenue",       "Float64", "NUMERIC(20,2)"),
        ColumnDef("gross_profit",  "Float64", "NUMERIC(20,2)"),
        ColumnDef("ebit",          "Float64", "NUMERIC(20,2)"),
        ColumnDef("ebt",           "Float64", "NUMERIC(20,2)"),
        ColumnDef("net_profit",    "Float64", "NUMERIC(20,2)"),
        ColumnDef("_ingested_at",  "Utf8",    "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_pipeline_run_id", "Utf8", "TEXT"),
    ],
)

GOLD_FACT_BALANCE_SHEET = TableDef(
    layer="gold", table_name="fact_balance_sheet",
    description="Fact bảng cân đối kế toán (normalized PostgreSQL)",
    primary_keys=["symbol", "report_type", "period"],
    columns=[
        ColumnDef("symbol",            "Utf8",    "TEXT",          nullable=False),
        ColumnDef("report_type",       "Utf8",    "TEXT",          nullable=False),
        ColumnDef("period",            "Utf8",    "TEXT",          nullable=False),
        ColumnDef("total_assets",      "Float64", "NUMERIC(20,2)"),
        ColumnDef("current_assets",    "Float64", "NUMERIC(20,2)"),
        ColumnDef("long_term_assets",  "Float64", "NUMERIC(20,2)"),
        ColumnDef("total_liabilities", "Float64", "NUMERIC(20,2)"),
        ColumnDef("short_term_debt",   "Float64", "NUMERIC(20,2)"),
        ColumnDef("long_term_debt",    "Float64", "NUMERIC(20,2)"),
        ColumnDef("equity",            "Float64", "NUMERIC(20,2)"),
        ColumnDef("_ingested_at",      "Utf8",    "TIMESTAMPTZ DEFAULT NOW()"),
        ColumnDef("_pipeline_run_id",  "Utf8",    "TEXT"),
    ],
)


# ===========================================================================
# ════════════════════════   REGISTRY DICTS   ════════════════════════════════
# ===========================================================================

ALL_BRONZE_TABLES: Dict[str, TableDef] = {
    "stock_prices":               BRONZE_TRADING_DATA,
    "company_profile":            BRONZE_COMPANY_PROFILE,
    "financial_ratios":           BRONZE_FINANCIAL_RATIOS,
    "income_statement":           BRONZE_INCOME_STATEMENT,
    "balance_sheet":              BRONZE_BALANCE_SHEET,
    "business_plan":              BRONZE_BUSINESS_PLAN,
    "industry_sectors":           BRONZE_INDUSTRY_SECTORS,
    "market_type_sectors":        BRONZE_MARKET_TYPE_SECTORS,
    "financial_report_summary":   BRONZE_FINANCIAL_REPORT_SUMMARY,
    "industry_info_summary_info": BRONZE_INDUSTRY_INFO_SUMMARY,
    "industry_info_financial_info": BRONZE_INDUSTRY_INFO_FINANCIAL,
    "industry_info_fund_info":    BRONZE_INDUSTRY_INFO_FUND,
}

ALL_SILVER_TABLES: Dict[str, TableDef] = {
    "fact_stock_price":         SILVER_FACT_STOCK_PRICE,
    "dim_company":              SILVER_DIM_COMPANY,
    "fact_financial_metrics":   SILVER_FACT_FINANCIAL_METRICS,
    "fact_business_plan":       SILVER_FACT_BUSINESS_PLAN,
    "fact_income_statement":    SILVER_FACT_INCOME_STATEMENT,
    "fact_balance_sheet":       SILVER_FACT_BALANCE_SHEET,
    "dim_industry":             SILVER_DIM_INDUSTRY,
    "dim_market_type":          SILVER_DIM_MARKET_TYPE,
    "fact_industry_summary":    SILVER_FACT_INDUSTRY_SUMMARY,
    "fact_financial_report":    SILVER_FACT_FINANCIAL_REPORT,
}

ALL_GOLD_TABLES: Dict[str, TableDef] = {
    "fact_stock_price":        GOLD_FACT_STOCK_PRICE,
    "dim_company":             GOLD_DIM_COMPANY,
    "fact_financial_metrics":  GOLD_FACT_FINANCIAL_METRICS,
    "fact_business_plan":      GOLD_FACT_BUSINESS_PLAN,
    "dim_industry":            GOLD_DIM_INDUSTRY,
    "dim_market_type":         GOLD_DIM_MARKET_TYPE,
    "fact_income_statement":   GOLD_FACT_INCOME_STATEMENT,
    "fact_balance_sheet":      GOLD_FACT_BALANCE_SHEET,
}


# ===========================================================================
# ═══════════════════   REGISTRY ACCESSOR FUNCTIONS   ════════════════════════
# ===========================================================================

def get_bronze_table(table_name: str) -> Optional[TableDef]:
    """Tra cứu Bronze TableDef theo table_name. Trả về None nếu không tìm thấy."""
    return ALL_BRONZE_TABLES.get(table_name)


def get_silver_table(table_name: str) -> Optional[TableDef]:
    """Tra cứu Silver TableDef theo table_name. Trả về None nếu không tìm thấy."""
    return ALL_SILVER_TABLES.get(table_name)


def get_gold_table(table_name: str) -> Optional[TableDef]:
    """Tra cứu Gold TableDef theo table_name. Trả về None nếu không tìm thấy."""
    return ALL_GOLD_TABLES.get(table_name)
