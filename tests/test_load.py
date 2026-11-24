import csv
import io
import json
from pathlib import Path
import sys
from unittest.mock import MagicMock, patch
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from src.extract.parser import flatten_country_row
from src.load import loader as load_module
from src.load.loader import (
    load_parquet_files,
    load_parquet_to_postgres,
    serialize_dataframe_chunk,
    serialize_records_chunk,
    stream_chunks_to_postgres,
    validate_loaded_batch_in_cur,
)
from src.storage.parquet_io import discover_parquet_files, write_records_to_parquet
from tests.helpers import assert_raises, make_fake_conn, make_indicator_record, resolve_dir


def test_serialize_chunks_and_batch_validation():
    df_sample = pd.DataFrame(
        [
            {"year": 2024, "value": 0.0, "decimal": 0, "note": "x, y\nz"},
            {"year": "sai", "value": pd.NA, "decimal": pd.NA, "note": "ok"},
        ]
    )
    serialized_df = serialize_dataframe_chunk(df_sample, ("year", "value", "decimal", "note"), "batch-1")
    assert serialized_df == '2024,0.0,0,"x, y\nz",batch-1\n,,,ok,batch-1\n'

    chunk_embedded = serialize_records_chunk([{"a": 0, "batch_id": "batch-embedded"}], ("a", "batch_id"), "ignored")
    assert chunk_embedded == "0,batch-embedded\n"

    assert_raises(
        ValueError,
        validate_loaded_batch_in_cur,
        MagicMock(),
        table_name="raw_worldbank_indicators",
        expected_rows=10,
        rows_loaded=3,
    )


def test_load_parquet_modes_and_watermark(tmp_path=None):
    target_dir = resolve_dir(tmp_path)
    records = [make_indicator_record(2024 - idx, float(idx)) for idx in range(5)]
    info = write_records_to_parquet(
        "raw_worldbank_indicators",
        iter(records),
        output_dir=target_dir,
        chunk_size=2,
        batch_id="batch-test",
        source_endpoint="https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD",
        ingestion_run_id="run-atomic",
        file_stem="raw_worldbank_indicators__NY.GDP.MKTP.CD",
    )

    replace_conn = make_fake_conn()
    rows_replace = load_parquet_to_postgres(
        replace_conn,
        info.path,
        batch_id="batch-test",
        schema="bronze",
        load_mode="replace",
        table_name="raw_worldbank_indicators",
        chunk_size=2,
        state_schema="elt_metadata",
        ingestion_run_id="run-atomic",
        task_key="raw_worldbank_indicators__NY.GDP.MKTP.CD",
        indicator_code="NY.GDP.MKTP.CD",
    )
    assert rows_replace == 5
    assert replace_conn.transaction_count == 1
    assert len(replace_conn.copied_queries[0][1]) == 3

    upsert_conn = make_fake_conn()
    rows_upsert = load_parquet_to_postgres(
        upsert_conn,
        info.path,
        batch_id="batch-test",
        schema="bronze",
        load_mode="upsert",
        table_name="raw_worldbank_indicators",
        chunk_size=2,
        state_schema="elt_metadata",
        ingestion_run_id="run-atomic",
        task_key="raw_worldbank_indicators__NY.GDP.MKTP.CD",
        indicator_code="NY.GDP.MKTP.CD",
    )
    assert rows_upsert == 5
    assert upsert_conn.transaction_count == 1
    upsert_sql = "\n".join(q for q, _ in upsert_conn.executed_queries)
    assert "CREATE TEMP TABLE" in upsert_sql
    assert "SELECT DISTINCT ON" in upsert_sql

    wm_calls = []
    with (
        patch.object(load_module, "update_load_checkpoint_in_cur", side_effect=lambda *a, **kw: None),
        patch.object(
            load_module,
            "upsert_watermark_in_cur",
            side_effect=lambda cur, state_schema, **kw: wm_calls.append(kw),
        ),
    ):
        load_parquet_to_postgres(
            make_fake_conn(),
            info.path,
            batch_id="batch-wm",
            schema="bronze",
            load_mode="upsert",
            table_name="raw_worldbank_indicators",
            chunk_size=2,
            state_schema="elt_metadata",
            ingestion_run_id="run-wm",
            task_key="raw_worldbank_indicators__NY.GDP.MKTP.CD",
            indicator_code="NY.GDP.MKTP.CD",
            watermark_year=2026,
            api_last_updated="2026-12-01",
        )
    assert len(wm_calls) == 1
    assert wm_calls[0]["last_loaded_year"] == 2024
    assert wm_calls[0]["api_last_updated"] == "2026-11-11"

    empty_info = write_records_to_parquet(
        "raw_worldbank_indicators",
        iter([]),
        output_dir=target_dir / "empty_wm",
        chunk_size=2,
        batch_id="batch-empty",
        source_endpoint="https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD",
        ingestion_run_id="run-empty-wm",
        file_stem="raw_worldbank_indicators__NY.GDP.MKTP.CD",
        allow_empty=True,
    )
    empty_wm_calls = []
    with (
        patch.object(load_module, "update_load_checkpoint_in_cur", side_effect=lambda *a, **kw: None),
        patch.object(
            load_module,
            "upsert_watermark_in_cur",
            side_effect=lambda cur, state_schema, **kw: empty_wm_calls.append(kw),
        ),
    ):
        load_parquet_to_postgres(
            make_fake_conn(),
            empty_info.path,
            batch_id="batch-empty",
            schema="bronze",
            load_mode="append",
            table_name="raw_worldbank_indicators",
            chunk_size=2,
            state_schema="elt_metadata",
            ingestion_run_id="run-empty-wm",
            task_key="raw_worldbank_indicators__NY.GDP.MKTP.CD",
            indicator_code="NY.GDP.MKTP.CD",
            watermark_year=2025,
            api_last_updated="2026-11-11",
        )
    assert empty_wm_calls == []

    null_year_info = write_records_to_parquet(
        "raw_worldbank_indicators",
        iter([make_indicator_record(None, 42.0)]),
        output_dir=target_dir / "null_year_wm",
        chunk_size=2,
        batch_id="batch-null-year",
        source_endpoint="https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD",
        ingestion_run_id="run-null-year-wm",
        file_stem="raw_worldbank_indicators__NY.GDP.MKTP.CD",
    )
    null_year_wm_calls = []
    with (
        patch.object(load_module, "update_load_checkpoint_in_cur", side_effect=lambda *a, **kw: None),
        patch.object(
            load_module,
            "upsert_watermark_in_cur",
            side_effect=lambda cur, state_schema, **kw: null_year_wm_calls.append(kw),
        ),
    ):
        loaded_null_year = load_parquet_to_postgres(
            make_fake_conn(),
            null_year_info.path,
            batch_id="batch-null-year",
            schema="bronze",
            load_mode="append",
            table_name="raw_worldbank_indicators",
            chunk_size=2,
            state_schema="elt_metadata",
            ingestion_run_id="run-null-year-wm",
            task_key="raw_worldbank_indicators__NY.GDP.MKTP.CD",
            indicator_code="NY.GDP.MKTP.CD",
            watermark_year=2025,
            api_last_updated="2026-12-01",
        )
    assert loaded_null_year == 1
    assert len(null_year_wm_calls) == 1
    assert null_year_wm_calls[0]["last_loaded_year"] is None
    assert null_year_wm_calls[0]["api_last_updated"] == "2026-11-11"

    assert_raises(
        ValueError,
        load_parquet_to_postgres,
        make_fake_conn(),
        info.path,
        batch_id="b",
        schema="bronze",
        load_mode="sai_mode",
    )
    assert_raises(
        FileNotFoundError,
        load_parquet_to_postgres,
        make_fake_conn(),
        target_dir / "khong_ton_tai.parquet",
        batch_id="b",
        schema="bronze",
        load_mode="replace",
    )
    assert info.path in discover_parquet_files(target_dir)


def test_load_legacy_parquet_populates_raw_payload(tmp_path=None):
    target_dir = resolve_dir(tmp_path)
    legacy_path = target_dir / "legacy_indicators.parquet"
    legacy_table = pa.table(
        {
            "country_iso2": ["VN", "US"],
            "country_iso3": ["VNM", "USA"],
            "country_name": ["Viet Nam", "United States"],
            "indicator_code": ["NY.GDP.MKTP.CD", "NY.GDP.MKTP.CD"],
            "indicator_name": ["GDP", "GDP"],
            "year": pa.array([2024, 2023], type=pa.int64()),
            "value": pa.array([100.5, None], type=pa.float64()),
            "unit": ["", None],
            "obs_status": ["", ""],
            "decimal": pa.array([0, 2], type=pa.int64()),
            "last_updated": ["2026-11-11", "2026-11-11"],
            "extracted_at": ["2026-11-11T00:00:00+00:00", "2026-11-11T00:00:00+00:00"],
            "batch_id": ["legacy-b1", "legacy-b1"],
            "source_endpoint": ["https://example.org", "https://example.org"],
            "ingestion_run_id": ["run-legacy", "run-legacy"],
        }
    )
    pq.write_table(legacy_table, legacy_path)

    legacy_conn = make_fake_conn()
    rows = load_parquet_to_postgres(
        legacy_conn,
        legacy_path,
        batch_id="legacy-b1",
        schema="bronze",
        load_mode="append",
        table_name="raw_worldbank_indicators",
        chunk_size=10,
    )
    assert rows == 2
    copy_sql, chunks = legacy_conn.copied_queries[0]
    assert "raw_payload" in copy_sql

    parsed_rows = list(csv.reader(io.StringIO("".join(chunks))))
    assert len(parsed_rows) == 2
    payload_0 = json.loads(parsed_rows[0][-1])
    payload_1 = json.loads(parsed_rows[1][-1])
    assert payload_0["country_iso2"] == "VN"
    assert payload_0["year"] == 2024
    assert payload_0["value"] == 100.5
    assert "batch_id" not in payload_0
    assert "extracted_at" not in payload_0
    assert payload_1["value"] is None
    assert payload_1["unit"] is None

    no_meta_conn = make_fake_conn()
    stream_chunks_to_postgres(
        no_meta_conn,
        table_name="raw_worldbank_countries",
        columns=("country_iso3", "country_iso2", "country_name"),
        chunks_iter=iter(
            [pd.DataFrame([{"country_iso3": "VNM", "country_iso2": "VN", "country_name": "Viet Nam"}])]
        ),
        batch_id="extra-batch",
        schema="bronze",
        load_mode="append",
    )
    nm_sql, nm_chunks = no_meta_conn.copied_queries[0]
    assert "raw_payload" in nm_sql
    assert "batch_id" in nm_sql
    nm_rows = list(csv.reader(io.StringIO("".join(nm_chunks))))
    assert nm_rows[0][-1] == "extra-batch"
    assert json.loads(nm_rows[0][-2]) == {
        "country_iso3": "VNM",
        "country_iso2": "VN",
        "country_name": "Viet Nam",
    }

    skip_conn = make_fake_conn()
    rows_skipped = load_parquet_to_postgres(
        skip_conn,
        legacy_path,
        batch_id="legacy-b1",
        schema="bronze",
        load_mode="append",
        table_name="raw_worldbank_indicators",
        chunk_size=10,
        auto_populate_raw_payload=False,
    )
    assert rows_skipped == 2
    skip_sql, _ = skip_conn.copied_queries[0]
    assert "raw_payload" not in skip_sql


def test_append_skips_duplicate_batch_id(tmp_path=None):
    target_dir = resolve_dir(tmp_path)
    records = [make_indicator_record(2024 - idx, float(idx)) for idx in range(3)]
    info = write_records_to_parquet(
        "raw_worldbank_indicators",
        iter(records),
        output_dir=target_dir,
        chunk_size=10,
        batch_id="batch-embedded",
        source_endpoint="https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD",
        ingestion_run_id="run-dup-test",
        file_stem="raw_worldbank_indicators__NY.GDP.MKTP.CD",
    )

    dup_conn = make_fake_conn()
    dup_conn.cursor_obj.fetchone.return_value = (True,)
    rows_skipped = load_parquet_to_postgres(
        dup_conn,
        info.path,
        batch_id="standalone-other",
        schema="bronze",
        load_mode="append",
        table_name="raw_worldbank_indicators",
        chunk_size=10,
        skip_duplicate_batch=True,
    )
    assert rows_skipped == 0
    assert dup_conn.copied_queries == []
    exists_queries = [(q, p) for q, p in dup_conn.executed_queries if "SELECT EXISTS" in q]
    assert len(exists_queries) == 1
    assert "indicator_code = %s" in exists_queries[0][0]
    assert exists_queries[0][1] == ("batch-embedded", "NY.GDP.MKTP.CD")

    country_dup_conn = make_fake_conn()
    country_dup_conn.cursor_obj.fetchone.return_value = (True,)
    country_skipped = stream_chunks_to_postgres(
        country_dup_conn,
        table_name="raw_worldbank_countries",
        columns=("country_iso3", "country_iso2", "country_name"),
        chunks_iter=iter([]),
        batch_id="batch-country",
        schema="bronze",
        load_mode="append",
        skip_duplicate_batch=True,
    )
    assert country_skipped == 0
    country_exists = [
        (q, p) for q, p in country_dup_conn.executed_queries if "SELECT EXISTS" in q
    ]
    assert len(country_exists) == 1
    assert "indicator_code" not in country_exists[0][0]
    assert country_exists[0][1] == ("batch-country",)

    force_conn = make_fake_conn()
    force_conn.cursor_obj.fetchone.return_value = (True,)
    rows_forced = load_parquet_to_postgres(
        force_conn,
        info.path,
        batch_id="standalone-other",
        schema="bronze",
        load_mode="append",
        table_name="raw_worldbank_indicators",
        chunk_size=10,
        skip_duplicate_batch=False,
    )
    assert rows_forced == 3
    assert len(force_conn.copied_queries) == 1


def test_ensure_table_flag_and_batch_ensure(tmp_path=None):
    target_dir = resolve_dir(tmp_path) / "ensure_flag"

    no_ensure_conn = make_fake_conn()
    loaded = stream_chunks_to_postgres(
        no_ensure_conn,
        table_name="raw_worldbank_indicators",
        columns=("country_iso2", "indicator_code", "year", "value"),
        chunks_iter=iter(
            [
                pd.DataFrame(
                    [
                        {
                            "country_iso2": "VN",
                            "indicator_code": "NY.GDP.MKTP.CD",
                            "year": 2024,
                            "value": 100.0,
                        }
                    ]
                )
            ]
        ),
        batch_id="batch-no-ensure",
        schema="bronze",
        load_mode="append",
        ensure_table=False,
    )
    assert loaded == 1
    no_ensure_sql = "\n".join(q for q, _ in no_ensure_conn.executed_queries)
    assert "CREATE TABLE IF NOT EXISTS" not in no_ensure_sql
    assert "ALTER TABLE" not in no_ensure_sql
    assert "CREATE INDEX" not in no_ensure_sql
    assert "information_schema.columns" not in no_ensure_sql

    ind_1 = write_records_to_parquet(
        "raw_worldbank_indicators",
        iter([make_indicator_record(2024, 10.0, code="NY.GDP.MKTP.CD")]),
        output_dir=target_dir,
        chunk_size=10,
        batch_id="batch-multi-1",
        file_stem="raw_worldbank_indicators__NY.GDP.MKTP.CD",
    )
    ind_2 = write_records_to_parquet(
        "raw_worldbank_indicators",
        iter([make_indicator_record(2024, 20.0, code="FP.CPI.TOTL.ZG")]),
        output_dir=target_dir,
        chunk_size=10,
        batch_id="batch-multi-2",
        file_stem="raw_worldbank_indicators__FP.CPI.TOTL.ZG",
    )
    country_file = write_records_to_parquet(
        "raw_worldbank_countries",
        iter([flatten_country_row({"id": "VNM", "iso2Code": "VN", "name": "Viet Nam"})]),
        output_dir=target_dir,
        chunk_size=10,
        batch_id="batch-country-1",
        file_stem="raw_worldbank_countries",
    )

    batch_conn = make_fake_conn()
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = batch_conn
    conn_cm.__exit__.return_value = False

    with (
        patch.object(load_module, "open_db_connection", return_value=conn_cm),
        patch.object(load_module, "ensure_raw_table", wraps=load_module.ensure_raw_table) as mock_ensure,
    ):
        counts = load_parquet_files(
            [ind_1.path, ind_2.path, country_file.path],
            batch_id="batch-fallback",
            schema="bronze",
            load_mode="append",
            chunk_size=10,
        )

    assert counts == {"raw_worldbank_indicators": 2, "raw_worldbank_countries": 1}
    assert mock_ensure.call_count == 2
    assert [c.args[2] for c in mock_ensure.call_args_list] == [
        "raw_worldbank_indicators",
        "raw_worldbank_countries",
    ]


def test_record_load_failure_falls_back_when_conn_closed():
    dead_conn = make_fake_conn()

    def broken_copy(q):
        dead_conn.closed = True
        raise RuntimeError("connection closed mid-copy")

    dead_conn.cursor_obj.copy.side_effect = broken_copy
    fallback_conn = make_fake_conn()
    fallback_cm = MagicMock()
    fallback_cm.__enter__.return_value = fallback_conn
    fallback_cm.__exit__.return_value = False

    with (
        patch.object(load_module, "open_db_connection", return_value=fallback_cm),
        patch.object(load_module, "record_load_failure") as mock_rec,
    ):
        assert_raises(
            RuntimeError,
            stream_chunks_to_postgres,
            dead_conn,
            table_name="raw_worldbank_indicators",
            columns=("country_iso2", "indicator_code", "year", "value"),
            chunks_iter=iter(
                [
                    pd.DataFrame(
                        [
                            {
                                "country_iso2": "VN",
                                "indicator_code": "NY.GDP.MKTP.CD",
                                "year": 2024,
                                "value": 1.0,
                            }
                        ]
                    )
                ]
            ),
            batch_id="b-dead",
            schema="bronze",
            load_mode="append",
            state_schema="elt_metadata",
            ingestion_run_id="r-dead",
            ensure_table=False,
        )

    assert mock_rec.call_count == 1
    assert mock_rec.call_args.args[0] is fallback_conn


if __name__ == "__main__":
    test_serialize_chunks_and_batch_validation()
    test_load_parquet_modes_and_watermark()
    test_load_legacy_parquet_populates_raw_payload()
    test_append_skips_duplicate_batch_id()
    test_ensure_table_flag_and_batch_ensure()
    test_record_load_failure_falls_back_when_conn_closed()
    print("LOAD TESTS PASSED")
