#!/usr/bin/env python3
"""
MinIO Silver/Bronze diagnostic probe.

Usage (from project root, inside ds_env):
  python scripts/probe_minio.py
  python scripts/probe_minio.py --prefix silver/fact_stock_price
  python scripts/probe_minio.py --prefix bronze/stock_prices --count 5
  python scripts/probe_minio.py --duckdb   # also test DuckDB httpfs S3 reads

Checks:
  1. MinIO reachability (boto3 / minio ping)
  2. Bucket 'lakehouse' exists
  3. Lists top-level silver/ and bronze/ prefixes
  4. Counts objects + shows latest partition for each key table path
  5. Optional: DuckDB httpfs read_parquet(...) smoke test
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv()

# ── MinIO / S3 credentials ────────────────────────────────────────────────────
# S3_ENDPOINT may be "localhost:9000" OR "http://localhost:9000" depending on
# which env var or .env entry is set.  Normalise to HOST:PORT for the minio
# client and for DuckDB (which must NOT receive the http:// prefix).
_raw_endpoint = os.getenv("S3_ENDPOINT", "localhost:9000")
# Strip any http:// or https:// prefix that may have been set in .env
MINIO_ENDPOINT = _raw_endpoint.replace("https://", "").replace("http://", "").rstrip("/")
MINIO_URL      = f"http://{MINIO_ENDPOINT}"          # full URL for minio-python / boto3
ACCESS_KEY     = os.getenv("AWS_ACCESS_KEY_ID",     "minioadmin")
SECRET_KEY     = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin_secure_123@#")
BUCKET         = os.getenv("LAKEHOUSE_BUCKET",      "lakehouse")

# Silver tables that gold models depend on
SILVER_PATHS = [
    "silver/fact_stock_price",
    "silver/dim_company",
    "silver/dim_industry",
    "silver/dim_market_type",
    "silver/fact_balance_sheet",
    "silver/fact_income_statement",
]
BRONZE_PATHS = [
    "bronze/stock_prices",
    "bronze/company_profiles",
    "bronze/financial_reports",
]

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

def ok(msg):  print(f"  {GREEN}✓{RESET}  {msg}")
def err(msg): print(f"  {RED}✗{RESET}  {msg}")
def warn(msg):print(f"  {YELLOW}⚠{RESET}  {msg}")


# ── 1. Probe MinIO via minio-python client ────────────────────────────────────

def probe_minio() -> bool:
    try:
        from minio import Minio
        from minio.error import S3Error
    except ImportError:
        warn("minio package not installed — skipping MinIO ping (pip install minio)")
        return _probe_via_boto3()

    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            secure=False,
        )
        buckets = client.list_buckets()
        ok(f"MinIO reachable at {MINIO_URL}")
        bucket_names = [b.name for b in buckets]
        if BUCKET in bucket_names:
            ok(f"Bucket '{BUCKET}' exists")
        else:
            err(f"Bucket '{BUCKET}' NOT FOUND — available: {bucket_names}")
            return False
        return True
    except Exception as exc:
        err(f"MinIO unreachable at {MINIO_URL}: {exc}")
        return False


def _probe_via_boto3() -> bool:
    try:
        import boto3
        from botocore.exceptions import ClientError, EndpointResolutionError
    except ImportError:
        warn("boto3 not installed either — cannot probe MinIO")
        return False

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1",
        )
        s3.head_bucket(Bucket=BUCKET)
        ok(f"MinIO reachable via boto3 at {MINIO_URL}, bucket '{BUCKET}' exists")
        return True
    except Exception as exc:
        err(f"MinIO unreachable via boto3: {exc}")
        return False


# ── 2. List objects under a prefix ───────────────────────────────────────────

def list_prefix(prefix: str, max_items: int = 20) -> list[str]:
    try:
        from minio import Minio
        client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
        objects = list(client.list_objects(BUCKET, prefix=prefix + "/", recursive=True))
        return [o.object_name for o in objects[:max_items]]
    except Exception:
        pass

    try:
        import boto3
        s3 = boto3.client(
            "s3", endpoint_url=MINIO_URL,
            aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1",
        )
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix + "/", MaxKeys=max_items)
        return [c["Key"] for c in resp.get("Contents", [])]
    except Exception:
        return []


def probe_paths(paths: list[str], label: str) -> dict[str, int]:
    print(f"\n{'─'*60}")
    print(f"  {label}")
    print(f"{'─'*60}")
    status: dict[str, int] = {}
    for path in paths:
        objects = list_prefix(path)
        parquets = [o for o in objects if o.endswith(".parquet")]
        count = len(parquets)
        status[path] = count
        if count > 0:
            latest = max(parquets)
            ok(f"{path:<45} {count:>4} parquet(s)  latest={latest.split('/')[-2] if '/' in latest else latest}")
        else:
            err(f"{path:<45}  NO parquet files found")
    return status


# ── 3. DuckDB httpfs smoke test ───────────────────────────────────────────────

def probe_duckdb(silver_paths_status: dict[str, int]) -> None:
    print(f"\n{'─'*60}")
    print("  DuckDB httpfs S3 smoke test")
    print(f"{'─'*60}")

    try:
        import duckdb
    except ImportError:
        warn("duckdb not installed — skipping DuckDB probe")
        return

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
        con.execute(f"SET s3_access_key_id='{ACCESS_KEY}';")
        con.execute(f"SET s3_secret_access_key='{SECRET_KEY}';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")
        con.execute("SET s3_region='us-east-1';")
        ok("DuckDB httpfs configured successfully")
    except Exception as exc:
        err(f"DuckDB httpfs setup failed: {exc}")
        con.close()
        return

    for path, count in silver_paths_status.items():
        if count == 0:
            warn(f"  Skipping {path} — no parquet files")
            continue
        s3_glob = f"s3://{BUCKET}/{path}/**/*.parquet"
        try:
            result = con.execute(
                f"SELECT COUNT(*) AS n FROM read_parquet('{s3_glob}', hive_partitioning=true)"
            ).fetchone()
            rows = result[0] if result else 0
            if rows > 0:
                ok(f"  {path:<40} → {rows:,} rows readable by DuckDB")
            else:
                warn(f"  {path:<40} → 0 rows (parquet files may be empty)")
        except Exception as exc:
            err(f"  {path:<40} → DuckDB read failed: {exc}")

    con.close()


# ── 4. Summarise what to run first ───────────────────────────────────────────

def print_summary(silver_status: dict[str, int], bronze_status: dict[str, int]) -> None:
    print(f"\n{'═'*60}")
    print("  DIAGNOSIS SUMMARY")
    print(f"{'═'*60}")

    all_silver_ready = all(c > 0 for c in silver_status.values())
    any_bronze = any(c > 0 for c in bronze_status.values())

    if all_silver_ready:
        ok("All silver tables have data — gold phase should succeed")
        print(f"\n  Run:  python scripts/deploy_full_pipeline.py gold --symbols HPG --env dev \\")
        print(f"          --config platforms/orchestration/prefect/config/cophieu68_config.yaml")
    else:
        missing_silver = [p for p, c in silver_status.items() if c == 0]
        err(f"Silver tables missing data: {missing_silver}")
        if any_bronze:
            warn("Bronze has data but silver is empty → run silver phase first")
            print(f"\n  Run:  python scripts/deploy_full_pipeline.py silver --env dev \\")
            print(f"          --config platforms/orchestration/prefect/config/cophieu68_config.yaml")
        else:
            err("Both bronze and silver are empty → run bronze first, then silver, then gold")
            print(f"\n  Run:  python scripts/deploy_full_pipeline.py full --symbols HPG --env dev \\")
            print(f"          --config platforms/orchestration/prefect/config/cophieu68_config.yaml")

    print(f"\n  MinIO endpoint:  {MINIO_URL}")
    print(f"  Bucket:          {BUCKET}")
    print(f"  Access key:      {ACCESS_KEY}")
    print(f"  S3_ENDPOINT env: {os.getenv('S3_ENDPOINT', '<not set, using default>')}")
    print(f"{'═'*60}\n")


# ── CLI entry point ───────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Probe MinIO silver/bronze data availability")
    p.add_argument("--prefix",  default=None, help="Single prefix to list (e.g. silver/fact_stock_price)")
    p.add_argument("--count",   type=int, default=20, help="Max objects to list per prefix")
    p.add_argument("--duckdb",  action="store_true", help="Run DuckDB httpfs smoke test")
    p.add_argument("--no-color", action="store_true", help="Disable colour output")
    return p.parse_args()


def main():
    args = parse_args()
    if args.no_color:
        global GREEN, RED, YELLOW, RESET
        GREEN = RED = YELLOW = RESET = ""

    print(f"\n{'═'*60}")
    print(f"  MinIO Lakehouse Diagnostic Probe")
    print(f"  Endpoint: {MINIO_URL}  Bucket: {BUCKET}")
    print(f"{'═'*60}")

    if not probe_minio():
        print(f"\n{RED}ERROR: Cannot reach MinIO. Check that docker-compose is running.{RESET}")
        print(f"  docker ps | grep minio")
        print(f"  docker-compose up -d minio\n")
        sys.exit(1)

    if args.prefix:
        objects = list_prefix(args.prefix, args.count)
        print(f"\n  Objects under '{args.prefix}' ({len(objects)} shown):")
        for o in objects:
            print(f"    {o}")
        return

    bronze_status = probe_paths(BRONZE_PATHS, "BRONZE layer")
    silver_status = probe_paths(SILVER_PATHS, "SILVER layer (Gold dependencies)")

    if args.duckdb:
        probe_duckdb(silver_status)

    print_summary(silver_status, bronze_status)


if __name__ == "__main__":
    main()
