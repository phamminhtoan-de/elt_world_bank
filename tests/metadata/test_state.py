from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.metadata import state as state_module
from tests.helpers import assert_raises, make_fake_conn


def test_state_schema_checkpoints_and_watermarks():
    assert state_module.normalize_task_spec(("raw_worldbank_countries", "https://api/country")) == (
        "raw_worldbank_countries",
        "raw_worldbank_countries",
        None,
        "https://api/country",
    )
    assert state_module.normalize_task_spec(
        ("tkey", "tbl", "NY.GDP.MKTP.CD", "https://api/ind")
    ) == ("tkey", "tbl", "NY.GDP.MKTP.CD", "https://api/ind")
    assert_raises(ValueError, state_module.normalize_task_spec, ("thieu_phan_tu",))

    state_conn = make_fake_conn()
    state_module.ensure_state_schema(state_conn, "elt_metadata")
    assert state_conn.transaction_count == 1
    ddl_sql = "\n".join(q for q, _ in state_conn.executed_queries)
    assert "ingestion_runs" in ddl_sql
    assert "ingestion_checkpoints" in ddl_sql
    assert "ingestion_step_logs" in ddl_sql
    assert "ingestion_watermarks" in ddl_sql

    wm_conn = make_fake_conn()
    with wm_conn.cursor() as cur:
        state_module.upsert_watermark_in_cur(
            cur,
            "elt_metadata",
            target_schema="bronze",
            task_key="raw_worldbank_indicators__NY.GDP.MKTP.CD",
            table_name="raw_worldbank_indicators",
            indicator_code="NY.GDP.MKTP.CD",
            last_loaded_year=2025,
            api_last_updated="  2026-11-11  ",
            last_run_id="run-1",
        )
    wm_sql, wm_params = wm_conn.executed_queries[0]
    assert "ON CONFLICT (target_schema, task_key) DO UPDATE" in wm_sql
    assert "GREATEST" in wm_sql
    assert wm_params[4] == 2025
    assert wm_params[5] == "2026-11-11"

    finish_conn = make_fake_conn()
    state_module.finish_run(
        finish_conn,
        "elt_metadata",
        ingestion_run_id="run-1",
        status="SUCCESS",
    )
    assert finish_conn.transaction_count == 1
    assert finish_conn.executed_queries[0][1][0] == "SUCCESS"


if __name__ == "__main__":
    test_state_schema_checkpoints_and_watermarks()
    print("STATE TESTS PASSED")
