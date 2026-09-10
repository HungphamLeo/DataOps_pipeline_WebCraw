-- =============================================================================
-- V1__create_schemas.sql
-- Tạo PG schemas cho staging + normalized layers
-- Idempotent — chạy lại không gây lỗi
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS normalized;
