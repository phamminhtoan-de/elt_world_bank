from contextlib import ExitStack, contextmanager
from pathlib import Path
import sys
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from main import run_ingestion
from src.extract.parser import flatten_country_row, flatten_indicator_meta_row
from src.load import load as load_module
from src.pipeline import recovery as recovery_module
from src.pipeline import runner as runner_module
from src.storage.parquet_io import write_records_to_parquet
from tests.helpers import assert_raises, make_fake_conn, make_indicator_record, resolve_dir


def make_pipeline_harness(*, fail_on_task=None):
    state_db = {"runs": {}, "checkpoints": {}, "run_order": []}
    extract_calls = {}
    extract_history = []
    opened_conns = []
    should_fail = {"value": fail_on_task is not None}

    @contextmanager
    def fake_open_db():
        conn = make_fake_conn()
        opened_conns.append(conn)
        try:
            yield conn
        finally:
            conn.close()

    def fake_get_resumable_run_id(conn, state_schema, run_id=None):
        if run_id:
            return run_id
        for rid in reversed(state_db["run_order"]):
            if state_db["runs"][rid]["status"] in ("RUNNING", "FAILED"):
                return rid
        return None

    def fake_get_previously_loaded_tasks(conn, state_schema, target_schema, exclude_run_id=None):
        loaded = set()
        for rid, cps in state_db["checkpoints"].items():
            if exclude_run_id and rid == exclude_run_id:
                continue
            for tkey, cp in cps.items():
                if cp.get("load_status") == "LOADED":
                    loaded.add(tkey)
        return loaded

    def fake_create_or_resume_run(
        conn, state_schema, *, table_specs, data_dir, auto_resume=True, run_id=None, **kwargs
    ):
        resolved = None
        resumed = False
        if auto_resume:
            for rid in reversed(state_db["run_order"]):
                if state_db["runs"][rid]["status"] in ("RUNNING", "FAILED"):
                    resolved = rid
                    resumed = True
                    break
        if not resolved:
            resolved = f"run-{len(state_db['run_order']) + 1}"
            run_dir = Path(data_dir)
            run_dir.mkdir(parents=True, exist_ok=True)
            state_db["run_order"].append(resolved)
            state_db["runs"][resolved] = {"status": "RUNNING", "data_dir": str(run_dir)}
            state_db["checkpoints"][resolved] = {
                tkey: {
                    "task_key": tkey,
                    "table_name": tbl,
                    "indicator_code": code,
                    "batch_id": f"{resolved}-b-{idx}",
                    "source_endpoint": ep,
                    "parquet_path": None,
                    "extract_status": "PENDING",
                    "load_status": "PENDING",
                    "rows_extracted": 0,
                    "rows_loaded": 0,
                    "run_data_dir": str(run_dir),
                }
                for idx, (tkey, tbl, code, ep) in enumerate(table_specs, start=1)
            }
        else:
            state_db["runs"][resolved]["status"] = "RUNNING"
        return resolved, resumed, state_db["checkpoints"][resolved]

    def fake_save_extract_cp(
        conn,
        state_schema,
        *,
        ingestion_run_id,
        parquet_path,
        rows_extracted,
        status="EXTRACTED",
        task_key=None,
        table_name="",
        **kwargs,
    ):
        cp = state_db["checkpoints"][ingestion_run_id][task_key or table_name]
        cp["extract_status"] = status
        cp["parquet_path"] = str(parquet_path) if parquet_path else None
        cp["rows_extracted"] = rows_extracted

    def fake_update_load_cp(
        cur, state_schema, *, ingestion_run_id, rows_loaded, status="LOADED", task_key=None, table_name="", **kwargs
    ):
        cp = state_db["checkpoints"][ingestion_run_id][task_key or table_name]
        cp["load_status"] = status
        cp["rows_loaded"] = rows_loaded

    def fake_finish_run(conn, state_schema, *, ingestion_run_id, status, **kwargs):
        state_db["runs"][ingestion_run_id]["status"] = status

    def fake_extract_task(
        session,
        task_key,
        table_name,
        indicator_code=None,
        *,
        output_dir=None,
        chunk_size=None,
        batch_id="",
        ingestion_run_id="",
        extracted_at=None,
        indicators=(),
        **kwargs,
    ):
        extract_calls[task_key] = extract_calls.get(task_key, 0) + 1
        extract_history.append(task_key)
        if should_fail["value"] and task_key == fail_on_task:
            raise RuntimeError(f"Simulated error at {task_key}")

        if table_name == "raw_worldbank_countries":
            rows = [flatten_country_row({"id": "VNM", "iso2Code": "VN", "name": "Viet Nam"})]
        elif table_name == "raw_worldbank_indicator_metadata":
            rows = [flatten_indicator_meta_row({"id": c, "name": c}, c) for c in indicators]
        else:
            rows = [make_indicator_record(2024, 100.0, code=indicator_code)]

        return write_records_to_parquet(
            table_name,
            iter(rows),
            output_dir=output_dir,
            chunk_size=chunk_size,
            batch_id=batch_id,
            source_endpoint="https://api.worldbank.org/v2",
            ingestion_run_id=ingestion_run_id,
            extracted_at=extracted_at,
            file_stem=task_key,
        )

    @contextmanager
    def patched_env(fetch_indicators_return=None):
        with ExitStack() as stack:
            if fetch_indicators_return is not None:
                mock_fetch = stack.enter_context(
                    patch.object(runner_module, "fetch_source_indicators", return_value=fetch_indicators_return)
                )
            else:
                mock_fetch = None
            stack.enter_context(patch.object(runner_module, "fetch_source_last_updated", return_value="2026-11-11"))
            stack.enter_context(patch.object(runner_module, "open_db_connection", side_effect=fake_open_db))
            stack.enter_context(patch.object(recovery_module, "open_db_connection", side_effect=fake_open_db))
            stack.enter_context(patch.object(runner_module, "ensure_state_schema", side_effect=lambda *a, **kw: None))
            stack.enter_context(patch.object(runner_module, "get_resumable_run_id", side_effect=fake_get_resumable_run_id))
            stack.enter_context(
                patch.object(runner_module, "get_previously_loaded_tasks", side_effect=fake_get_previously_loaded_tasks)
            )
            stack.enter_context(patch.object(runner_module, "get_watermarks", return_value={}))
            stack.enter_context(patch.object(runner_module, "create_or_resume_run", side_effect=fake_create_or_resume_run))
            stack.enter_context(patch.object(runner_module, "save_extract_checkpoint", side_effect=fake_save_extract_cp))
            stack.enter_context(patch.object(recovery_module, "save_extract_checkpoint", side_effect=fake_save_extract_cp))
            stack.enter_context(patch.object(runner_module, "finish_run", side_effect=fake_finish_run))
            stack.enter_context(patch.object(recovery_module, "finish_run", side_effect=fake_finish_run))
            stack.enter_context(patch.object(runner_module, "extract_task_to_parquet", side_effect=fake_extract_task))
            stack.enter_context(patch.object(load_module, "update_load_checkpoint_in_cur", side_effect=fake_update_load_cp))
            stack.enter_context(patch.object(load_module, "upsert_watermark_in_cur", side_effect=lambda *a, **kw: None))
            yield mock_fetch

    return {
        "state_db": state_db,
        "extract_calls": extract_calls,
        "extract_history": extract_history,
        "opened_conns": opened_conns,
        "should_fail": should_fail,
        "patched_env": patched_env,
    }


def test_checkpoint_and_auto_resume(tmp_path=None):
    target_dir = resolve_dir(tmp_path)
    test_indicators = ("NY.GDP.MKTP.CD", "FP.CPI.TOTL.ZG")
    harness = make_pipeline_harness(fail_on_task="raw_worldbank_indicators__FP.CPI.TOTL.ZG")

    assert_raises(ValueError, run_ingestion, data_dir=target_dir, load_mode="che_do_sai")
    assert_raises(ValueError, run_ingestion, data_dir=target_dir, max_indicators=-1)

    with harness["patched_env"](fetch_indicators_return=test_indicators) as mock_fetch:
        assert_raises(
            RuntimeError,
            run_ingestion,
            data_dir=target_dir,
            load_mode="upsert",
            chunk_size=2,
            max_indicators=2,
        )
        assert mock_fetch.call_count == 1
        assert harness["state_db"]["runs"]["run-1"]["status"] == "FAILED"

        harness["should_fail"]["value"] = False
        results = run_ingestion(data_dir=target_dir, load_mode="upsert", chunk_size=2, indicators=test_indicators)

    assert all(c.closed for c in harness["opened_conns"])
    assert results == {
        "raw_worldbank_countries": 1,
        "raw_worldbank_indicator_metadata": 2,
        "raw_worldbank_indicators": 2,
    }
    assert harness["extract_calls"] == {
        "raw_worldbank_countries": 1,
        "raw_worldbank_indicator_metadata": 1,
        "raw_worldbank_indicators__NY.GDP.MKTP.CD": 1,
        "raw_worldbank_indicators__FP.CPI.TOTL.ZG": 2,
    }
    assert harness["state_db"]["runs"]["run-1"]["status"] == "SUCCESS"


def test_incremental_new_indicators_and_fresh(tmp_path=None):
    target_dir = resolve_dir(tmp_path) / "incremental"
    harness = make_pipeline_harness()

    with harness["patched_env"]():
        res1 = run_ingestion(
            data_dir=target_dir,
            load_mode="append",
            chunk_size=2,
            indicators=("NY.GDP.MKTP.CD", "FP.CPI.TOTL.ZG"),
        )
        assert res1 == {
            "raw_worldbank_countries": 1,
            "raw_worldbank_indicator_metadata": 2,
            "raw_worldbank_indicators": 2,
        }
        assert len(harness["extract_history"]) == 4

        harness["extract_history"].clear()
        res2 = run_ingestion(
            data_dir=target_dir,
            load_mode="append",
            chunk_size=2,
            indicators=("NY.GDP.MKTP.CD", "FP.CPI.TOTL.ZG", "SP.POP.TOTL"),
        )
        assert harness["extract_history"] == [
            "raw_worldbank_indicator_metadata",
            "raw_worldbank_indicators__SP.POP.TOTL",
        ]
        assert res2 == {
            "raw_worldbank_indicator_metadata": 3,
            "raw_worldbank_indicators": 1,
        }

        harness["extract_history"].clear()
        res3 = run_ingestion(
            data_dir=target_dir,
            load_mode="append",
            chunk_size=2,
            indicators=("NY.GDP.MKTP.CD", "FP.CPI.TOTL.ZG", "SP.POP.TOTL"),
        )
        assert res3 == {}
        assert harness["extract_history"] == []
        assert len(harness["state_db"]["run_order"]) == 2

        harness["extract_history"].clear()
        res4 = run_ingestion(
            data_dir=target_dir,
            load_mode="append",
            chunk_size=2,
            auto_resume=False,
            indicators=("NY.GDP.MKTP.CD", "FP.CPI.TOTL.ZG", "SP.POP.TOTL"),
        )
        assert len(harness["extract_history"]) == 5
        assert res4 == {
            "raw_worldbank_countries": 1,
            "raw_worldbank_indicator_metadata": 3,
            "raw_worldbank_indicators": 3,
        }


def test_corrupt_parquet_auto_reextract(tmp_path=None):
    target_dir = resolve_dir(tmp_path) / "corrupt_recovery"
    harness = make_pipeline_harness(fail_on_task="raw_worldbank_indicator_metadata")

    with harness["patched_env"]():
        assert_raises(
            RuntimeError,
            run_ingestion,
            data_dir=target_dir,
            load_mode="replace",
            chunk_size=2,
            indicators=("NY.GDP.MKTP.CD",),
        )
        countries_file = Path(
            harness["state_db"]["checkpoints"]["run-1"]["raw_worldbank_countries"]["parquet_path"]
        )
        countries_file.write_text("file_bi_hong_khong_phai_parquet", encoding="utf-8")

        harness["should_fail"]["value"] = False
        res = run_ingestion(
            data_dir=target_dir,
            load_mode="replace",
            chunk_size=2,
            indicators=("NY.GDP.MKTP.CD",),
        )

    assert harness["extract_calls"]["raw_worldbank_countries"] == 2
    assert res == {
        "raw_worldbank_countries": 1,
        "raw_worldbank_indicator_metadata": 1,
        "raw_worldbank_indicators": 1,
    }
    assert harness["state_db"]["runs"]["run-1"]["status"] == "SUCCESS"


def test_recover_metadata_from_file_ready_parquet(tmp_path=None):
    target_dir = resolve_dir(tmp_path) / "recover_metadata"
    harness = make_pipeline_harness()
    save_calls = {"count": 0}
    saved_statuses = []

    def flaky_save_extract_cp(
        conn,
        state_schema,
        *,
        ingestion_run_id,
        parquet_path,
        rows_extracted,
        status="EXTRACTED",
        task_key=None,
        table_name="",
        **kwargs,
    ):
        save_calls["count"] += 1
        resolved_key = task_key or table_name
        saved_statuses.append((resolved_key, status))
        cp = harness["state_db"]["checkpoints"][ingestion_run_id][resolved_key]
        if resolved_key == "raw_worldbank_countries" and status == "EXTRACTED" and save_calls["count"] <= 3:
            raise RuntimeError("Simulated DB disconnect after Parquet ready")
        cp["extract_status"] = status
        cp["parquet_path"] = str(parquet_path) if parquet_path else None
        cp["rows_extracted"] = rows_extracted

    with harness["patched_env"]():
        with (
            patch.object(recovery_module, "save_extract_checkpoint", side_effect=flaky_save_extract_cp),
            patch.object(recovery_module.time, "sleep", side_effect=lambda *_: None),
        ):
            assert_raises(
                RuntimeError,
                run_ingestion,
                data_dir=target_dir,
                load_mode="append",
                chunk_size=2,
                indicators=("NY.GDP.MKTP.CD",),
            )
            countries_file = target_dir / "raw_worldbank_countries.parquet"
            assert countries_file.exists()
            assert harness["extract_calls"]["raw_worldbank_countries"] == 1
            assert (
                harness["state_db"]["checkpoints"]["run-1"]["raw_worldbank_countries"]["extract_status"]
                == "FILE_READY"
            )

            results = run_ingestion(
                data_dir=target_dir,
                load_mode="append",
                chunk_size=2,
                indicators=("NY.GDP.MKTP.CD",),
            )

    assert harness["extract_calls"]["raw_worldbank_countries"] == 1
    assert results == {
        "raw_worldbank_countries": 1,
        "raw_worldbank_indicator_metadata": 1,
        "raw_worldbank_indicators": 1,
    }
    assert ("raw_worldbank_countries", "FILE_READY") in saved_statuses
    assert harness["state_db"]["runs"]["run-1"]["status"] == "SUCCESS"


if __name__ == "__main__":
    test_checkpoint_and_auto_resume()
    test_incremental_new_indicators_and_fresh()
    test_corrupt_parquet_auto_reextract()
    test_recover_metadata_from_file_ready_parquet()
    print("PIPELINE FLOW TESTS PASSED")
