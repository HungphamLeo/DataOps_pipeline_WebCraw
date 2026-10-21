-- =============================================================================
-- V3__create_normalized_tables.sql
-- Normalized tables: typed columns, PK constraints, ON CONFLICT upsert support
-- Đây là serving layer cuối cùng — các bảng chuẩn hóa cho reporting/API
-- =============================================================================

-- ---------------------------------------------------------------------------
-- normalized.fact_stock_price
-- Fact bảng giá giao dịch lịch sử cổ phiếu
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS normalized.fact_stock_price (
    trade_key            TEXT             NOT NULL PRIMARY KEY,
    symbol               TEXT             NOT NULL,
    trade_date           DATE,
    close_price          NUMERIC(15,2),
    open_price           NUMERIC(15,2),
    high_price           NUMERIC(15,2),
    low_price            NUMERIC(15,2),
    volume               BIGINT,
    foreign_buy          BIGINT,
    foreign_sell         BIGINT,
    foreign_net_value    NUMERIC(20,2),
    year                 SMALLINT,
    month                SMALLINT,
    _ingested_at         TIMESTAMPTZ      DEFAULT NOW(),
    _pipeline_run_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_stock_price_symbol
    ON normalized.fact_stock_price (symbol);
CREATE INDEX IF NOT EXISTS idx_fact_stock_price_trade_date
    ON normalized.fact_stock_price (trade_date);
CREATE INDEX IF NOT EXISTS idx_fact_stock_price_year_month
    ON normalized.fact_stock_price (year, month);

-- ---------------------------------------------------------------------------
-- normalized.dim_company
-- Dimension công ty — SCD Type 2
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS normalized.dim_company (
    company_key              TEXT         NOT NULL PRIMARY KEY,
    symbol                   TEXT         NOT NULL,
    full_name                TEXT,
    english_name             TEXT,
    short_name               TEXT,
    address                  TEXT,
    phone_number             TEXT,
    fax                      TEXT,
    website                  TEXT,
    email_address            TEXT,
    established_date         DATE,
    listed_date              DATE,
    listed_volume_initial    BIGINT,
    listed_volume            BIGINT,
    circulating_volume       BIGINT,
    market_capitalization    NUMERIC(20,2),
    foreign_buy              TEXT,
    foreign_ownership        TEXT,
    effective_date           DATE,
    end_date                 DATE,
    is_current               BOOLEAN      DEFAULT TRUE,
    _row_hash                TEXT,
    _ingested_at             TIMESTAMPTZ  DEFAULT NOW(),
    _pipeline_run_id         TEXT
);

CREATE INDEX IF NOT EXISTS idx_dim_company_symbol
    ON normalized.dim_company (symbol);
CREATE INDEX IF NOT EXISTS idx_dim_company_is_current
    ON normalized.dim_company (is_current);

-- ---------------------------------------------------------------------------
-- normalized.fact_financial_metrics
-- Fact chỉ số tài chính mỗi mã cổ phiếu
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS normalized.fact_financial_metrics (
    financial_ratio_key  TEXT             NOT NULL PRIMARY KEY,
    symbol               TEXT             NOT NULL,
    reference_price      NUMERIC(15,2),
    open_price           NUMERIC(15,2),
    high_price           NUMERIC(15,2),
    low_price            NUMERIC(15,2),
    volume               BIGINT,
    book_value           NUMERIC(15,2),
    eps                  NUMERIC(15,4),
    pe                   NUMERIC(10,2),
    pb                   NUMERIC(10,2),
    roe                  NUMERIC(10,4),
    roa                  NUMERIC(10,4),
    beta                 NUMERIC(10,4),
    market_cap           NUMERIC(25,2),
    listed_volume        BIGINT,
    avg_volume_52w       BIGINT,
    debt                 NUMERIC(20,2),
    equity               NUMERIC(20,2),
    debt_to_equity       NUMERIC(10,4),
    equity_to_assets     NUMERIC(10,4),
    cash                 NUMERIC(20,2),
    update_time          TIMESTAMPTZ      DEFAULT NOW(),
    _ingested_at         TIMESTAMPTZ      DEFAULT NOW(),
    _pipeline_run_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_financial_metrics_symbol
    ON normalized.fact_financial_metrics (symbol);

-- ---------------------------------------------------------------------------
-- normalized.fact_business_plan
-- Fact kế hoạch kinh doanh theo năm
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS normalized.fact_business_plan (
    plan_key                 TEXT         NOT NULL PRIMARY KEY,
    symbol                   TEXT         NOT NULL,
    year                     SMALLINT,
    plan_revenue             NUMERIC(20,2),
    pass_revenue             NUMERIC(20,2),
    plan_profit              NUMERIC(20,2),
    pass_profit              NUMERIC(20,2),
    revenue_completion_rate  NUMERIC(10,4),
    profit_completion_rate   NUMERIC(10,4),
    update_time              TIMESTAMPTZ  DEFAULT NOW(),
    _ingested_at             TIMESTAMPTZ  DEFAULT NOW(),
    _pipeline_run_id         TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_business_plan_symbol_year
    ON normalized.fact_business_plan (symbol, year);

-- ---------------------------------------------------------------------------
-- normalized.dim_industry
-- Dimension ngành nghề — SCD Type 2
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS normalized.dim_industry (
    industry_sk          TEXT             NOT NULL PRIMARY KEY,
    industry_code        TEXT             NOT NULL,
    industry_name        TEXT,
    effective_date       DATE,
    end_date             DATE,
    is_current           BOOLEAN          DEFAULT TRUE,
    _row_hash            TEXT,
    _ingested_at         TIMESTAMPTZ      DEFAULT NOW(),
    _pipeline_run_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_dim_industry_code
    ON normalized.dim_industry (industry_code);

-- ---------------------------------------------------------------------------
-- normalized.dim_market_type
-- Dimension loại thị trường HOSE/HNX/UPCOM/VN30
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS normalized.dim_market_type (
    market_key           TEXT             NOT NULL PRIMARY KEY,
    market_type          TEXT,
    market_name          TEXT,
    update_time          TIMESTAMPTZ      DEFAULT NOW(),
    _ingested_at         TIMESTAMPTZ      DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- normalized.fact_income_statement
-- Fact báo cáo kết quả kinh doanh
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS normalized.fact_income_statement (
    symbol               TEXT             NOT NULL,
    report_type          TEXT             NOT NULL,
    period               TEXT             NOT NULL,
    revenue              NUMERIC(20,2),
    gross_profit         NUMERIC(20,2),
    ebit                 NUMERIC(20,2),
    ebt                  NUMERIC(20,2),
    net_profit           NUMERIC(20,2),
    _ingested_at         TIMESTAMPTZ      DEFAULT NOW(),
    _pipeline_run_id     TEXT,
    CONSTRAINT pk_fact_income_statement PRIMARY KEY (symbol, report_type, period)
);

CREATE INDEX IF NOT EXISTS idx_fact_income_statement_symbol
    ON normalized.fact_income_statement (symbol);

-- ---------------------------------------------------------------------------
-- normalized.fact_balance_sheet
-- Fact bảng cân đối kế toán
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS normalized.fact_balance_sheet (
    symbol               TEXT             NOT NULL,
    report_type          TEXT             NOT NULL,
    period               TEXT             NOT NULL,
    total_assets         NUMERIC(20,2),
    current_assets       NUMERIC(20,2),
    long_term_assets     NUMERIC(20,2),
    total_liabilities    NUMERIC(20,2),
    short_term_debt      NUMERIC(20,2),
    long_term_debt       NUMERIC(20,2),
    equity               NUMERIC(20,2),
    _ingested_at         TIMESTAMPTZ      DEFAULT NOW(),
    _pipeline_run_id     TEXT,
    CONSTRAINT pk_fact_balance_sheet PRIMARY KEY (symbol, report_type, period)
);

CREATE INDEX IF NOT EXISTS idx_fact_balance_sheet_symbol
    ON normalized.fact_balance_sheet (symbol);
