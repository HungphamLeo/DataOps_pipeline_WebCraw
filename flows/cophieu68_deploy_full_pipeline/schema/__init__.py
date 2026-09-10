"""
schema/ — Single Source of Truth cho Data Modelling
=====================================================
Mọi table definition (Bronze / Silver / Gold) đều khai báo tại đây.
Import dùng trong bronze.py, silver.py, serving.py để:
  - Enforce Polars schema khi write_parquet
  - Generate PostgreSQL DDL cho staging + normalized tables
  - Không hardcode column names rải rác khắp codebase
"""
from flows.cophieu68_deploy_full_pipeline.schema.schema_registry import (
    # Data classes
    ColumnDef,
    TableDef,
    # Registry functions
    get_bronze_table,
    get_silver_table,
    get_gold_table,
    # Registry dicts
    ALL_BRONZE_TABLES,
    ALL_SILVER_TABLES,
    ALL_GOLD_TABLES,
)

__all__ = [
    "ColumnDef",
    "TableDef",
    "get_bronze_table",
    "get_silver_table",
    "get_gold_table",
    "ALL_BRONZE_TABLES",
    "ALL_SILVER_TABLES",
    "ALL_GOLD_TABLES",
]
