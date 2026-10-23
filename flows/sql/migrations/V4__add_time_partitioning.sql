-- =============================================================================
-- Migration: V4__add_time_partitioning.sql
-- Scope: ST07 — Advanced Time-Series Optimization for normalized.fact_stock_price
-- =============================================================================
--
-- [KIẾN TRÚC & TỐI ƯU HÓA (Senior Engineering Rationale)]:
-- 1. Declarative Range Partitioning (PARTITION BY RANGE (trade_date)):
--    - Vấn đề: Khi bảng vượt ngưỡng 10M+ rows, câu lệnh scan time-range gây I/O bottleneck.
--    - Tối ưu: Phân vùng theo năm (hoặc quý/tháng). PostgreSQL Query Planner sẽ kích hoạt
--      Partition Pruning (loại trừ ngay các partition ngoài phạm vi WHERE trade_date),
--      giảm I/O tới 80-90% cho các truy vấn theo chu kỳ.
--    - Lưu ý kỹ thuật PostgreSQL: Khi dùng Declarative Partitioning, mọi PRIMARY KEY hoặc
--      UNIQUE CONSTRAINT bắt buộc phải bao gồm cả Partition Key -> PK (trade_key, trade_date).
--
-- 2. B-Tree Covering Index (Index-Only Scan via INCLUDE clause):
--    - Tối ưu: Dashboard/API thường xuyên truy vấn: 
--        SELECT close_price, open_price, volume FROM fact_stock_price 
--        WHERE symbol = 'VIC' ORDER BY trade_date DESC LIMIT 30;
--    - Covering Index chứa (symbol, trade_date DESC) ở B-Tree search key, và đính kèm các
--      cột payload (close_price, volume, ...) ở lá leaf node qua từ khóa INCLUDE.
--    - Hiệu năng: Đạt Zero Heap Fetches (Index-Only Scan), không tốn Random I/O đọc bảng gốc.
-- =============================================================================

-- Bước 1: Sao lưu / Đổi tên bảng non-partitioned cũ nếu đang tồn tại
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_tables 
        WHERE schemaname = 'normalized' AND tablename = 'fact_stock_price'
        AND NOT EXISTS (
            SELECT 1 FROM pg_partitioned_table pt 
            JOIN pg_class c ON pt.partrelid = c.oid 
            JOIN pg_namespace n ON c.relnamespace = n.oid 
            WHERE n.nspname = 'normalized' AND c.relname = 'fact_stock_price'
        )
    ) THEN
        -- Đổi tên để tránh conflict, cho phép migrate dữ liệu lịch sử nếu cần
        ALTER TABLE normalized.fact_stock_price RENAME TO fact_stock_price_legacy;
    END IF;
END $$;

-- Bước 2: Tạo Bảng Phân Vùng Cha (Declarative Partitioned Parent Table)
CREATE TABLE IF NOT EXISTS normalized.fact_stock_price (
    trade_key            TEXT             NOT NULL,
    symbol               TEXT             NOT NULL,
    trade_date           DATE             NOT NULL, -- Partition Key
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
    _pipeline_run_id     TEXT,
    -- Điểm tối ưu: Composite PK bao gồm trade_date để đáp ứng chuẩn PG Partitioning
    CONSTRAINT pk_fact_stock_price PRIMARY KEY (trade_key, trade_date)
) PARTITION BY RANGE (trade_date);

-- Bước 3: Tạo các phân vùng cụ thể (Partitions) theo năm
-- [Tối ưu]: Cắt lát theo năm giúp thao tác bảo trì (DROP/DETACH PARTITION) diễn ra tức thì (O(1))
CREATE TABLE IF NOT EXISTS normalized.fact_stock_price_y2023 
    PARTITION OF normalized.fact_stock_price
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE IF NOT EXISTS normalized.fact_stock_price_y2024 
    PARTITION OF normalized.fact_stock_price
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS normalized.fact_stock_price_y2025 
    PARTITION OF normalized.fact_stock_price
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS normalized.fact_stock_price_y2026 
    PARTITION OF normalized.fact_stock_price
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- Phân vùng Default: Bắt tất cả dữ liệu lịch sử cũ hoặc tương lai chưa có partition riêng
CREATE TABLE IF NOT EXISTS normalized.fact_stock_price_default 
    PARTITION OF normalized.fact_stock_price DEFAULT;

-- Bước 4: Tạo Covering Index (Áp dụng tự động xuống tất cả partitions con)
-- [Điểm tối ưu]: Index-Only Scan cho truy vấn chuỗi thời gian phân tích kỹ thuật
CREATE INDEX IF NOT EXISTS idx_fact_stock_price_covering
    ON normalized.fact_stock_price (symbol, trade_date DESC)
    INCLUDE (close_price, open_price, high_price, low_price, volume);

-- Index phụ phục vụ tổng hợp phân tích theo chu kỳ tháng/năm
CREATE INDEX IF NOT EXISTS idx_fact_stock_price_ym
    ON normalized.fact_stock_price (year, month);