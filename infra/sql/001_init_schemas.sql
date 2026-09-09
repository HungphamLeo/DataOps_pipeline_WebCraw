-- =============================================================================
-- PostgreSQL Schema Migration
-- Vietnam Stock Data Platform
-- =============================================================================
-- Luồng: MinIO (bronze parquet) → staging schema (raw load) → normalized schema
--
-- staging.*   : dữ liệu raw từ MinIO, giữ nguyên kiểu TEXT để dễ import
-- normalized.*: dữ liệu đã dedup, cast type, check null, chuẩn hóa
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0. Create schemas
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS normalized;

-- ---------------------------------------------------------------------------
-- 1. STAGING layer — raw tables (tất cả TEXT, không constraint)
-- ---------------------------------------------------------------------------

-- 1.1 Giao dịch lịch sử
CREATE TABLE IF NOT EXISTS staging.stock_prices (
    _batch_id           TEXT,
    _run_id             TEXT,
    _ingest_timestamp   TEXT,
    ingest_date         TEXT,
    symbol              TEXT,
    "date"              TEXT,
    close_price         TEXT,
    volume              TEXT,
    open_price          TEXT,
    high_price          TEXT,
    low_price           TEXT,
    foreign_buy         TEXT,
    foreign_sell        TEXT,
    foreign_value       TEXT,
    _dq_status          TEXT,
    _dq_errors          TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW()
);

-- 1.2 Hồ sơ công ty
CREATE TABLE IF NOT EXISTS staging.company_profile (
    _batch_id           TEXT,
    _run_id             TEXT,
    _ingest_timestamp   TEXT,
    ingest_date         TEXT,
    symbol              TEXT,
    full_name           TEXT,
    english_name        TEXT,
    short_name          TEXT,
    address             TEXT,
    phone_number        TEXT,
    fax                 TEXT,
    website             TEXT,
    email_address       TEXT,
    established_date    TEXT,
    listed_date         TEXT,
    listed_volume_initial TEXT,
    listed_volume       TEXT,
    circulating_volume  TEXT,
    market_capitalization TEXT,
    foreign_buy         TEXT,
    foreign_ownership   TEXT,
    current_price       TEXT,
    _dq_status          TEXT,
    _dq_errors          TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW()
);

-- 1.3 Chỉ số tài chính
CREATE TABLE IF NOT EXISTS staging.financial_ratios (
    _batch_id           TEXT,
    _run_id             TEXT,
    _ingest_timestamp   TEXT,
    ingest_date         TEXT,
    symbol              TEXT,
    reference_price     TEXT,
    open_price          TEXT,
    high_price          TEXT,
    low_price           TEXT,
    volume              TEXT,
    book_value          TEXT,
    eps                 TEXT,
    pe                  TEXT,
    pb                  TEXT,
    roe                 TEXT,
    roa                 TEXT,
    roa_roe             TEXT,
    beta                TEXT,
    market_cap          TEXT,
    listed_volume       TEXT,
    avg_volume_52w      TEXT,
    high_low_52w        TEXT,
    debt                TEXT,
    equity              TEXT,
    debt_to_equity      TEXT,
    equity_to_assets    TEXT,
    cash                TEXT,
    eps_power           TEXT,
    roe_power           TEXT,
    invest_efficiency   TEXT,
    pb_power            TEXT,
    price_growth_power  TEXT,
    _dq_status          TEXT,
    _dq_errors          TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW()
);

-- 1.4 Kế hoạch kinh doanh
CREATE TABLE IF NOT EXISTS staging.business_plan (
    _batch_id           TEXT,
    _run_id             TEXT,
    _ingest_timestamp   TEXT,
    ingest_date         TEXT,
    symbol              TEXT,
    year                TEXT,
    plan_revenue        TEXT,
    pass_revenue        TEXT,
    plan_profit         TEXT,
    pass_profit         TEXT,
    _dq_status          TEXT,
    _dq_errors          TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW()
);

-- 1.5 Ngành nghề công ty
CREATE TABLE IF NOT EXISTS staging.industry_sectors (
    _batch_id           TEXT,
    _run_id             TEXT,
    _ingest_timestamp   TEXT,
    ingest_date         TEXT,
    industry_code       TEXT,
    industry_name       TEXT,
    symbol              TEXT,
    company_name        TEXT,
    close_price         TEXT,
    increase_decrease   TEXT,
    volumn24h           TEXT,
    volumn52w           TEXT,
    listed_volumn       TEXT,
    market_capitalization TEXT,
    _dq_status          TEXT,
    _dq_errors          TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW()
);

-- 1.6 Loại thị trường
CREATE TABLE IF NOT EXISTS staging.market_type_sectors (
    _batch_id           TEXT,
    _run_id             TEXT,
    _ingest_timestamp   TEXT,
    ingest_date         TEXT,
    market_type_code    TEXT,
    market_type_name    TEXT,
    symbol              TEXT,
    company_name        TEXT,
    close_price         TEXT,
    increase_decrease   TEXT,
    volumn24h           TEXT,
    volumn52w           TEXT,
    listed_volumn       TEXT,
    market_capitalization TEXT,
    _dq_status          TEXT,
    _dq_errors          TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW()
);

-- 1.7 Báo cáo tóm tắt tài chính
CREATE TABLE IF NOT EXISTS staging.financial_report_summary (
    _batch_id           TEXT,
    _run_id             TEXT,
    _ingest_timestamp   TEXT,
    ingest_date         TEXT,
    symbol              TEXT,
    report_type         TEXT,
    data_json           TEXT,
    _dq_status          TEXT,
    _dq_errors          TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- 2. NORMALIZED layer — chuẩn hóa kiểu dữ liệu, dedup, constraints
-- ---------------------------------------------------------------------------

-- 2.1 Dim: Công ty
CREATE TABLE IF NOT EXISTS normalized.dim_company (
    company_key         TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,
    full_name           TEXT,
    english_name        TEXT,
    short_name          TEXT,
    address             TEXT,
    phone_number        TEXT,
    fax                 TEXT,
    website             TEXT,
    email_address       TEXT,
    established_date    DATE,
    listed_date         DATE,
    listed_volume_initial BIGINT,
    listed_volume       BIGINT,
    circulating_volume  BIGINT,
    market_capitalization NUMERIC(20,2),
    foreign_buy         TEXT,
    foreign_ownership   TEXT,
    effective_date      DATE NOT NULL,
    end_date            DATE,
    is_current          BOOLEAN DEFAULT TRUE,
    _row_hash           TEXT,
    _ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    _pipeline_run_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_dim_company_symbol ON normalized.dim_company(symbol);
CREATE INDEX IF NOT EXISTS idx_dim_company_is_current ON normalized.dim_company(is_current);

-- 2.2 Dim: Ngành
CREATE TABLE IF NOT EXISTS normalized.dim_industry (
    industry_sk         TEXT PRIMARY KEY,
    industry_code       TEXT NOT NULL,
    industry_name       TEXT,
    effective_date      DATE,
    end_date            DATE,
    is_current          BOOLEAN DEFAULT TRUE,
    _row_hash           TEXT,
    _ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    _pipeline_run_id    TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_industry_code ON normalized.dim_industry(industry_code);

-- 2.3 Dim: Loại thị trường
CREATE TABLE IF NOT EXISTS normalized.dim_market_type (
    market_key          TEXT PRIMARY KEY,
    market_type         TEXT NOT NULL,
    market_name         TEXT,
    description         TEXT,
    update_time         TIMESTAMPTZ,
    _ingested_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_market_type ON normalized.dim_market_type(market_type);

-- 2.4 Fact: Giao dịch lịch sử
CREATE TABLE IF NOT EXISTS normalized.fact_stock_price (
    trade_key           TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,
    trade_date          DATE NOT NULL,
    close_price         NUMERIC(15,2),
    open_price          NUMERIC(15,2),
    high_price          NUMERIC(15,2),
    low_price           NUMERIC(15,2),
    volume              BIGINT,
    foreign_buy         BIGINT,
    foreign_sell        BIGINT,
    foreign_net_value   NUMERIC(20,2),
    year                SMALLINT,
    month               SMALLINT,
    _ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    _pipeline_run_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_sp_symbol ON normalized.fact_stock_price(symbol);
CREATE INDEX IF NOT EXISTS idx_fact_sp_date ON normalized.fact_stock_price(trade_date);
CREATE INDEX IF NOT EXISTS idx_fact_sp_symbol_date ON normalized.fact_stock_price(symbol, trade_date);

-- 2.5 Fact: Chỉ số tài chính
CREATE TABLE IF NOT EXISTS normalized.fact_financial_metrics (
    financial_ratio_key TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,
    reference_price     NUMERIC(15,2),
    open_price          NUMERIC(15,2),
    high_price          NUMERIC(15,2),
    low_price           NUMERIC(15,2),
    volume              BIGINT,
    book_value          NUMERIC(15,2),
    eps                 NUMERIC(15,4),
    pe                  NUMERIC(10,2),
    pb                  NUMERIC(10,2),
    roe                 NUMERIC(10,4),
    roa                 NUMERIC(10,4),
    beta                NUMERIC(10,4),
    market_cap          NUMERIC(25,2),
    listed_volume       BIGINT,
    avg_volume_52w      BIGINT,
    high_52w            NUMERIC(15,2),
    low_52w             NUMERIC(15,2),
    debt                NUMERIC(20,2),
    equity              NUMERIC(20,2),
    debt_to_equity      NUMERIC(10,4),
    equity_to_assets    NUMERIC(10,4),
    cash                NUMERIC(20,2),
    update_time         TIMESTAMPTZ,
    _ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    _pipeline_run_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_fm_symbol ON normalized.fact_financial_metrics(symbol);

-- 2.6 Fact: Kế hoạch kinh doanh
CREATE TABLE IF NOT EXISTS normalized.fact_business_plan (
    plan_key            TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,
    year                SMALLINT NOT NULL,
    plan_revenue        NUMERIC(20,2),
    pass_revenue        NUMERIC(20,2),
    plan_profit         NUMERIC(20,2),
    pass_profit         NUMERIC(20,2),
    revenue_completion_rate NUMERIC(8,4),
    profit_completion_rate  NUMERIC(8,4),
    update_time         TIMESTAMPTZ,
    _ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    _pipeline_run_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_bp_symbol ON normalized.fact_business_plan(symbol);

-- 2.7 Fact: Báo cáo kết quả kinh doanh (denormalized để dễ query)
CREATE TABLE IF NOT EXISTS normalized.fact_income_statement (
    income_key          TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,
    time_report_type    TEXT NOT NULL,  -- ANNUALLY | QUARTERLY
    report_type         TEXT,
    data_json           TEXT,
    update_time         TIMESTAMPTZ,
    _ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    _pipeline_run_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_is_symbol ON normalized.fact_income_statement(symbol);
CREATE INDEX IF NOT EXISTS idx_fact_is_type ON normalized.fact_income_statement(time_report_type);

-- 2.8 Fact: Bảng cân đối kế toán
CREATE TABLE IF NOT EXISTS normalized.fact_balance_sheet (
    balance_key         TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,
    time_report_type    TEXT NOT NULL,
    report_type         TEXT,
    data_json           TEXT,
    update_time         TIMESTAMPTZ,
    _ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    _pipeline_run_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_bs_symbol ON normalized.fact_balance_sheet(symbol);

-- 2.9 Fact: Thông tin ngành tổng hợp
CREATE TABLE IF NOT EXISTS normalized.fact_industry_summary (
    industry_summary_key TEXT PRIMARY KEY,
    industry_code        TEXT,
    industry_name        TEXT,
    industry_metric_type TEXT,
    industry_index       NUMERIC(15,4),
    percentage_change    NUMERIC(10,4),
    liquidity            NUMERIC(20,2),
    total_capital        NUMERIC(20,2),
    average_price        NUMERIC(15,2),
    book_value           NUMERIC(15,2),
    eps                  NUMERIC(15,4),
    pe                   NUMERIC(10,2),
    roa                  NUMERIC(10,4),
    roe                  NUMERIC(10,4),
    supply_volumn        BIGINT,
    total_asset          NUMERIC(25,2),
    total_equity         NUMERIC(20,2),
    total_liabilities    NUMERIC(20,2),
    percentage_debt_on_equity   NUMERIC(10,4),
    percentage_equity_on_assets NUMERIC(10,4),
    revenue              NUMERIC(20,2),
    profit_before_tax    NUMERIC(20,2),
    update_time          TIMESTAMPTZ,
    _ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    _pipeline_run_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_ind_code ON normalized.fact_industry_summary(industry_code);
