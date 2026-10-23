-- =============================================================================
-- Migration: V5__scd2_dim_company.sql
-- Scope: ST07 — Slowly Changing Dimension (SCD Type 2) for normalized.dim_company
-- =============================================================================
--
-- [KIẾN TRÚC & TỐI ƯU HÓA (Kimball Enterprise DW Pattern)]:
-- 1. Theo dõi lịch sử biến động (Point-in-Time Accuracy):
--    - Thông tin doanh nghiệp (vốn điều lệ, số cổ phiếu lưu hành, tên, địa chỉ)
--      thay đổi theo thời gian.
--    - Nếu dùng SCD Type 1 (UPDATE ghi đè) -> Mất lịch sử, các phép tính P/E, P/B quá khứ bị sai.
--    - Chuẩn hóa SCD Type 2 với 3 trường cốt lõi:
--        * valid_from (DATE): Ngày bắt đầu có hiệu lực
--        * valid_to (DATE): Ngày hết hiệu lực (NULL nếu đang active)
--        * is_current (BOOLEAN): Cờ đánh dấu bản ghi hiện tại (TRUE/FALSE)
--
-- 2. Tối ưu hóa Reconcile qua Stored Procedure:
--    - Toàn bộ logic Expire bản ghi cũ và Insert bản ghi mới được đóng gói trong một transaction
--      nguyên tử (ACID) trên database, loại bỏ Round-trip latency giữa Python và Postgres.
--    - Sử dụng mã băm `_row_hash` (MD5/SHA256 các cột thuộc tính) để phát hiện thay đổi (Delta Detection)
--      với độ phức tạp O(1) so sánh chuỗi thay vì so khớp từng cột đơn lẻ.
-- =============================================================================

-- Bước 1: Bổ sung các cột SCD Type 2 vào bảng normalized.dim_company nếu chưa có
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'normalized' AND table_name = 'dim_company' AND column_name = 'valid_from'
    ) THEN
        ALTER TABLE normalized.dim_company ADD COLUMN valid_from DATE DEFAULT CURRENT_DATE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'normalized' AND table_name = 'dim_company' AND column_name = 'valid_to'
    ) THEN
        ALTER TABLE normalized.dim_company ADD COLUMN valid_to DATE DEFAULT NULL;
    END IF;
END $$;

-- Bước 2: Tạo Partial Index tối ưu hóa truy vấn các bản ghi đang Active (is_current = TRUE)
-- [Điểm tối ưu]: Index này chỉ index các dòng active, kích thước cực nhỏ và tốc độ lookup O(log N)
CREATE INDEX IF NOT EXISTS idx_dim_company_active_symbol
    ON normalized.dim_company (symbol)
    WHERE is_current = TRUE;

-- Bước 3: Stored Procedure thực hiện SCD Type 2 Atomic Reconciliation
CREATE OR REPLACE PROCEDURE normalized.sp_merge_dim_company_scd2(p_run_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_today DATE := CURRENT_DATE;
BEGIN
    -- ─────────────────────────────────────────────────────────────────────────
    -- PHA 1: EXPIRE OLD RECORDS (Đóng phiên bản cũ)
    -- Khi phát hiện _row_hash giữa staging và bản ghi is_current=TRUE trong DW bị lệch,
    -- cập nhật valid_to về ngày hôm qua và chuyển is_current = FALSE.
    -- ─────────────────────────────────────────────────────────────────────────
    UPDATE normalized.dim_company d
    SET 
        valid_to = v_today - INTERVAL '1 day',
        is_current = FALSE,
        _pipeline_run_id = p_run_id
    FROM staging.company_profile s
    WHERE d.symbol = UPPER(s.symbol)
      AND d.is_current = TRUE
      AND s._row_hash IS NOT NULL
      AND d._row_hash IS DISTINCT FROM s._row_hash;

    -- ─────────────────────────────────────────────────────────────────────────
    -- PHA 2: INSERT NEW VERSION / NEW RECORD (Chèn phiên bản mới)
    -- Chèn các doanh nghiệp mới xuất hiện hoặc doanh nghiệp vừa bị expire ở Pha 1.
    -- Surrogate Key (company_key) được sinh duy nhất theo symbol + valid_from.
    -- ─────────────────────────────────────────────────────────────────────────
    INSERT INTO normalized.dim_company (
        company_key,
        symbol,
        full_name,
        english_name,
        short_name,
        address,
        phone_number,
        fax,
        website,
        email_address,
        established_date,
        listed_date,
        listed_volume_initial,
        listed_volume,
        circulating_volume,
        market_capitalization,
        foreign_buy,
        foreign_ownership,
        effective_date,
        valid_from,
        valid_to,
        is_current,
        _row_hash,
        _ingested_at,
        _pipeline_run_id
    )
    SELECT DISTINCT ON (UPPER(s.symbol))
        -- Surrogate Key kết hợp giữa Symbol và ngày bắt đầu hiệu lực
        MD5(UPPER(s.symbol) || '_' || v_today::TEXT) AS company_key,
        UPPER(s.symbol),
        NULLIF(s.full_name, 'None'),
        NULLIF(s.english_name, 'None'),
        NULLIF(s.short_name, 'None'),
        NULLIF(s.address, 'None'),
        NULLIF(s.phone_number, 'None'),
        NULLIF(s.fax, 'None'),
        NULLIF(s.website, 'None'),
        NULLIF(s.email_address, 'None'),
        CASE WHEN s.established_date ~ '^\d{4}-\d{2}-\d{2}$' THEN s.established_date::DATE ELSE NULL END,
        CASE WHEN s.listed_date ~ '^\d{4}-\d{2}-\d{2}$' THEN s.listed_date::DATE ELSE NULL END,
        NULLIF(s.listed_volume_initial, 'None')::BIGINT,
        NULLIF(s.listed_volume, 'None')::BIGINT,
        NULLIF(s.circulating_volume, 'None')::BIGINT,
        NULLIF(s.market_capitalization, 'None')::NUMERIC(20,2),
        NULLIF(s.foreign_buy, 'None'),
        NULLIF(s.foreign_ownership, 'None'),
        v_today,
        v_today,
        NULL::DATE,
        TRUE,
        s._row_hash,
        NOW(),
        p_run_id
    FROM staging.company_profile s
    LEFT JOIN normalized.dim_company d
        ON UPPER(s.symbol) = d.symbol AND d.is_current = TRUE
    WHERE s.symbol IS NOT NULL AND s.symbol != 'None'
      AND (d.symbol IS NULL OR d._row_hash IS DISTINCT FROM s._row_hash)
    ORDER BY UPPER(s.symbol), s._ingest_timestamp DESC NULLS LAST;

END;
$$;