from pathlib import Path
import sys
from src.reliability import runs as runs_module
from src.reliability import schema as schema_module
from src.reliability import watermarks as watermarks_module
from tests.helpers import assert_raises, make_fake_conn


def test_state_schema_checkpoints_and_watermarks():
    assert schema_module.normalize_task_spec(("raw_worldbank_countries", "https://api/country")) == (
        "raw_worldbank_countries",
        "raw_worldbank_countries",
        None,
        "https://api/country",
    )
    assert schema_module.normalize_task_spec(
        ("tkey", "tbl", "NY.GDP.MKTP.CD", "https://api/ind")
    ) == ("tkey", "tbl", "NY.GDP.MKTP.CD", "https://api/ind")
    assert_raises(ValueError, schema_module.normalize_task_spec, ("thieu_phan_tu",))

    state_conn = make_fake_conn()
    schema_module.ensure_state_schema(state_conn, "elt_metadata")
    assert state_conn.transaction_count == 1
    ddl_sql = "\n".join(q for q, _ in state_conn.executed_queries)
    assert "ingestion_runs" in ddl_sql
    assert "ingestion_checkpoints" in ddl_sql
    assert "ingestion_step_logs" in ddl_sql
    assert "ingestion_watermarks" in ddl_sql
    assert "ALTER TABLE" in ddl_sql
    assert "UPDATE" in ddl_sql

    ready_state_conn = make_fake_conn()
    ready_state_conn.cursor_obj.fetchall.return_value = [
        ("ingestion_checkpoints", "task_key"),
        ("ingestion_checkpoints", "indicator_code"),
        ("ingestion_step_logs", "task_key"),
    ]
    schema_module.ensure_state_schema(ready_state_conn, "elt_metadata")
    ready_ddl_sql = "\n".join(q for q, _ in ready_state_conn.executed_queries)
    assert "ALTER TABLE" not in ready_ddl_sql
    assert "UPDATE" not in ready_ddl_sql

    run_conn = make_fake_conn()
    run_id, resumed, _ = runs_module.create_or_resume_run(
        run_conn,
        "elt_metadata",
        table_specs=(
            ("t1", "raw_worldbank_countries", None, "https://api/country"),
            ("t2", "raw_worldbank_indicators__NY.GDP.MKTP.CD", "NY.GDP.MKTP.CD", "https://api/ind"),
        ),
        load_mode="append",
        target_schema="bronze",
        data_dir="data",
        chunk_size=100,
        auto_resume=False,
    )
    assert resumed is False
    assert run_id
    assert run_conn.cursor_obj.executemany.call_count == 1
    assert len(run_conn.cursor_obj.executemany.call_args[0][1]) == 2

    wm_conn = make_fake_conn()
    with wm_conn.cursor() as cur:
        watermarks_module.upsert_watermark_in_cur(
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
    assert wm_sql.count("GREATEST") == 2
    assert "last_loaded_year = GREATEST" in wm_sql
    assert "api_last_updated = GREATEST" in wm_sql
    assert wm_params[4] == 2025
    assert wm_params[5] == "2026-11-11"

    finish_conn = make_fake_conn()
    runs_module.finish_run(
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
