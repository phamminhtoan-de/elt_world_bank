from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

from src.storage import database as db_module
from src.storage.database import (
    TABLE_DDL_COLUMNS,
    TABLE_PRIMARY_KEYS,
    backfill_missing_raw_payloads,
    ensure_raw_table,
    ensure_schema,
    open_db_connection,
    table_ref,
)
from tests.helpers import assert_raises, make_fake_conn


def test_database_ddl_and_connection_lifecycle():
    assert set(TABLE_DDL_COLUMNS.keys()) == {
        "raw_worldbank_indicators",
        "raw_worldbank_countries",
        "raw_worldbank_indicator_metadata",
    }
    assert TABLE_PRIMARY_KEYS["raw_worldbank_indicators"] == (
        "country_iso2",
        "indicator_code",
        "year",
    )

    ref_text = repr(table_ref("bronze", "raw_worldbank_indicators"))
    assert "bronze" in ref_text
    assert "raw_worldbank_indicators" in ref_text

    conn = make_fake_conn()
    with conn.cursor() as cur:
        ensure_schema(cur, "bronze")
        ensure_raw_table(cur, "bronze", "raw_worldbank_indicators")

    executed_sql = "\n".join(q for q, _ in conn.executed_queries)
    assert "CREATE SCHEMA IF NOT EXISTS" in executed_sql
    assert "CREATE TABLE IF NOT EXISTS" in executed_sql
    assert "ADD COLUMN IF NOT EXISTS" in executed_sql
    assert "CREATE INDEX IF NOT EXISTS" in executed_sql
    assert "raw_payload" in executed_sql
    assert "JSONB" in executed_sql
    assert "ingested_at" in executed_sql

    assert_raises(ValueError, ensure_raw_table, MagicMock(), "bronze", "bang_khong_ton_tai")

    fake_conn = make_fake_conn()
    with patch.object(db_module, "get_connection", return_value=fake_conn):
        try:
            with open_db_connection() as active_conn:
                assert active_conn is fake_conn
                raise RuntimeError("boom")
        except RuntimeError:
            pass
    assert fake_conn.closed is True


def test_backfill_missing_raw_payloads():
    conn = make_fake_conn()
    conn.cursor_obj.rowcount = 7
    counts = backfill_missing_raw_payloads(conn, "bronze")

    assert conn.transaction_count == 1
    assert len(counts) == len(TABLE_DDL_COLUMNS)
    assert all(counts[tbl] == 7 for tbl in TABLE_DDL_COLUMNS)

    executed_sql = "\n".join(q for q, _ in conn.executed_queries)
    assert "information_schema.tables" in executed_sql
    assert "ADD COLUMN IF NOT EXISTS" in executed_sql
    assert "to_jsonb(tbl) - %s::text[]" in executed_sql
    assert "WHERE tbl.raw_payload IS NULL" in executed_sql

    update_params = [params for q, params in conn.executed_queries if "UPDATE" in q]
    assert len(update_params) == len(TABLE_DDL_COLUMNS)
    excluded_cols = update_params[0][0]
    assert set(excluded_cols) == {
        "raw_payload",
        "extracted_at",
        "batch_id",
        "source_endpoint",
        "ingestion_run_id",
        "ingested_at",
    }

    missing_conn = make_fake_conn()
    missing_conn.cursor_obj.fetchone.return_value = None
    missing_counts = backfill_missing_raw_payloads(missing_conn, "bronze")
    assert all(missing_counts[tbl] == 0 for tbl in TABLE_DDL_COLUMNS)
    assert not any("UPDATE" in q for q, _ in missing_conn.executed_queries)

    custom_conn = make_fake_conn()
    custom_conn.cursor_obj.rowcount = 3
    custom_counts = backfill_missing_raw_payloads(
        custom_conn,
        "bronze",
        tables=("raw_worldbank_countries",),
        exclude_meta_columns=("raw_payload", "batch_id"),
        only_null=False,
    )
    assert custom_counts == {"raw_worldbank_countries": 3}
    custom_sql = "\n".join(q for q, _ in custom_conn.executed_queries)
    assert "WHERE tbl.raw_payload IS NULL" not in custom_sql
    custom_update_params = [params for q, params in custom_conn.executed_queries if "UPDATE" in q]
    assert custom_update_params == [(["raw_payload", "batch_id"],)]

    assert_raises(
        ValueError,
        backfill_missing_raw_payloads,
        make_fake_conn(),
        "bronze",
        tables=("bang_khong_ton_tai",),
    )


if __name__ == "__main__":
    test_database_ddl_and_connection_lifecycle()
    test_backfill_missing_raw_payloads()
    print("DATABASE TESTS PASSED")
