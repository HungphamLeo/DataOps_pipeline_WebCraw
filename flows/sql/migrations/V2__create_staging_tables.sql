-- =============================================================================
-- V2__create_staging_tables.sql
-- Staging tables: tất cả cột là TEXT để load raw từ Parquet
-- Không có PK constraint — chỉ là buffer trung gian trước khi process
-- =============================================================================

-- staging.fact_stock_price
CREATE TABLE IF NOT EXISTS staging.fact_stock_price (
    trade_key            TEXT,
    symbol               TEXT,
    date                 TEXT,
    close_price          TEXT,
    open_price           TEXT,
    high_price           TEXT,
    low_price            TEXT,
    volume               TEXT,
    foreign_buy          TEXT,
    foreign_sell         TEXT,
    foreign_value        TEXT,
    year                 TEXT,
    month                TEXT,
    _ingested_at         TEXT,
    _pipeline_run_id     TEXT,
    _ingest_timestamp    TEXT,
    _batch_id            TEXT,
    _run_id              TEXT,
    _dq_status           TEXT,
    _dq_errors           TEXT
);

-- staging.company_profile (→ normalized.dim_company)
CREATE TABLE IF NOT EXISTS staging.company_profile (
    company_key              TEXT,
    symbol                   TEXT,
    full_name                TEXT,
    english_name             TEXT,
    short_name               TEXT,
    address                  TEXT,
    phone_number             TEXT,
    fax                      TEXT,
    website                  TEXT,
    email_address            TEXT,
    established_date         TEXT,
    listed_date              TEXT,
    listed_volume_initial    TEXT,
    listed_volume            TEXT,
    circulating_volume       TEXT,
    market_capitalization    TEXT,
    foreign_buy              TEXT,
    foreign_ownership        TEXT,
    effective_date           TEXT,
    _row_hash                TEXT,
    _ingested_at             TEXT,
    _pipeline_run_id         TEXT,
    _ingest_timestamp        TEXT,
    _batch_id                TEXT,
    _run_id                  TEXT
);

-- staging.financial_ratios (→ normalized.fact_financial_metrics)
CREATE TABLE IF NOT EXISTS staging.financial_ratios (
    financial_ratio_key  TEXT,
    symbol               TEXT,
    reference_price      TEXT,
    open_price           TEXT,
    high_price           TEXT,
    low_price            TEXT,
    volume               TEXT,
    book_value           TEXT,
    eps                  TEXT,
    pe                   TEXT,
    pb                   TEXT,
    roe                  TEXT,
    roa                  TEXT,
    beta                 TEXT,
    market_cap           TEXT,
    listed_volume        TEXT,
    avg_volume_52w       TEXT,
    high_low_52w         TEXT,
    debt                 TEXT,
    equity               TEXT,
    debt_to_equity       TEXT,
    equity_to_assets     TEXT,
    cash                 TEXT,
    _ingested_at         TEXT,
    _pipeline_run_id     TEXT,
    _ingest_timestamp    TEXT,
    _batch_id            TEXT,
    _run_id              TEXT
);

-- staging.business_plan (→ normalized.fact_business_plan)
CREATE TABLE IF NOT EXISTS staging.business_plan (
    plan_key             TEXT,
    symbol               TEXT,
    "Year"               TEXT,
    "Plan_revenue"       TEXT,
    "Pass_revenue"       TEXT,
    "Plan_profit"        TEXT,
    "Pass_profit"        TEXT,
    _ingested_at         TEXT,
    _pipeline_run_id     TEXT,
    _ingest_timestamp    TEXT,
    _batch_id            TEXT,
    _run_id              TEXT
);

-- staging.industry_sectors (→ normalized.dim_industry)
CREATE TABLE IF NOT EXISTS staging.industry_sectors (
    industry_sk          TEXT,
    industry_code        TEXT,
    industry_name        TEXT,
    effective_date       TEXT,
    _row_hash            TEXT,
    _ingested_at         TEXT,
    _pipeline_run_id     TEXT,
    _ingest_timestamp    TEXT,
    _batch_id            TEXT,
    _run_id              TEXT
);

-- staging.market_type_sectors (→ normalized.dim_market_type)
CREATE TABLE IF NOT EXISTS staging.market_type_sectors (
    market_key           TEXT,
    market_type_code     TEXT,
    market_type_name     TEXT,
    _ingested_at         TEXT,
    _pipeline_run_id     TEXT,
    _ingest_timestamp    TEXT,
    _batch_id            TEXT,
    _run_id              TEXT
);

-- staging.fact_income_statement
CREATE TABLE IF NOT EXISTS staging.fact_income_statement (
    symbol               TEXT,
    report_type          TEXT,
    period               TEXT,
    revenue              TEXT,
    gross_profit         TEXT,
    ebit                 TEXT,
    ebt                  TEXT,
    net_profit           TEXT,
    _ingested_at         TEXT,
    _pipeline_run_id     TEXT,
    _ingest_timestamp    TEXT,
    _batch_id            TEXT,
    _run_id              TEXT
);

-- staging.fact_balance_sheet
CREATE TABLE IF NOT EXISTS staging.fact_balance_sheet (
    symbol               TEXT,
    report_type          TEXT,
    period               TEXT,
    total_assets         TEXT,
    current_assets       TEXT,
    long_term_assets     TEXT,
    total_liabilities    TEXT,
    short_term_debt      TEXT,
    long_term_debt       TEXT,
    equity               TEXT,
    _ingested_at         TEXT,
    _pipeline_run_id     TEXT,
    _ingest_timestamp    TEXT,
    _batch_id            TEXT,
    _run_id              TEXT
);
