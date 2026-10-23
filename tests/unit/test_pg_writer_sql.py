"""Test ST02: PostgreSQLWriter — mock pool để không cần PostgreSQL."""
from unittest.mock import MagicMock, patch
import pytest

from platforms.storage.postgre.base_postgre import PostgreSQLWriter


@pytest.fixture
def writer():
    with patch("platforms.storage.postgre.base_postgre.pg_pool.ThreadedConnectionPool") as pool_cls:
        pool = pool_cls.return_value
        w = PostgreSQLWriter(
            host="h", port=5432, database="d", username="u", password="p",
            metrics_interval=0,            # tắt thread nền trong test
            logger=MagicMock(),
        )
        w._test_pool = pool
        yield w


def test_ping_returns_false_when_cursor_raises(writer):
    conn = MagicMock()
    conn.cursor.side_effect = Exception("connection closed")
    assert writer._ping(conn) is False


def test_conn_reconnects_when_stale(writer):
    stale = MagicMock()
    stale.cursor.side_effect = Exception("dropped")
    writer._test_pool.getconn.return_value = stale
    fresh = MagicMock()
    with patch.object(writer, "_fresh_conn", return_value=fresh):
        with writer._conn() as conn:
            assert conn is fresh
    writer._test_pool.putconn.assert_called_once_with(stale, close=True)
    fresh.commit.assert_called_once()
    fresh.close.assert_called_once()


def test_upsert_builds_on_conflict_sql(writer):
    conn = MagicMock()
    writer._test_pool.getconn.return_value = conn
    res = writer.upsert(
        "public.t", [{"id": 1, "v": 2}], conflict_columns=["id"]
    )
    sql = conn.cursor.return_value.__enter__.return_value.executemany.call_args[0][0]
    assert 'ON CONFLICT ("id") DO UPDATE SET "v" = EXCLUDED."v"' in sql
    assert res == {"ok": True, "upserted_count": 1}


def test_upsert_empty_data_is_noop(writer):
    assert writer.upsert("public.t", [], ["id"]) == {"ok": True, "upserted_count": 0}

def test_v4_partitioning_syntax_and_covering_index(writer):
    """Kiểm tra câu lệnh partition và covering index được compile hợp lệ."""
    with open("flows/sql/migrations/V4__add_time_partitioning.sql", "r", encoding="utf-8") as f:
        sql = f.read()
    assert "PARTITION BY RANGE (trade_date)" in sql
    assert "idx_fact_stock_price_covering" in sql
    assert "INCLUDE (close_price, open_price, high_price, low_price, volume)" in sql


def test_v5_scd2_procedure_syntax(writer):
    """Kiểm tra procedure SCD2 chứa đầy đủ các trường Kimball."""
    with open("flows/sql/migrations/V5__scd2_dim_company.sql", "r", encoding="utf-8") as f:
        sql = f.read()
    assert "sp_merge_dim_company_scd2" in sql
    assert "valid_to = v_today - INTERVAL '1 day'" in sql
    assert "is_current = FALSE" in sql
    assert "is_current = TRUE" in sql