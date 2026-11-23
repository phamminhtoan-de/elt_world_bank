from pathlib import Path
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.storage.parquet_io import (
    cleanup_stale_temp_parquet_files,
    iter_parquet_dataframe_chunks,
    read_parquet_file_batch_id,
    validate_parquet_file,
    write_records_to_parquet,
)
from tests.helpers import assert_raises, make_indicator_record, resolve_dir


def test_write_and_iter_parquet_chunks(tmp_path=None):
    target_dir = resolve_dir(tmp_path)
    rows = iter(
        [
            make_indicator_record(2024, 0.0),
            make_indicator_record(2023, None),
            make_indicator_record("sai_nam", 200.0),
        ]
    )

    info = write_records_to_parquet(
        "raw_worldbank_indicators",
        rows,
        output_dir=target_dir,
        chunk_size=2,
        batch_id="batch-1",
        source_endpoint="https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD",
        ingestion_run_id="run-1",
        extracted_at="2026-11-23T06:00:00+00:00",
        file_stem="raw_worldbank_indicators__NY.GDP.MKTP.CD",
    )

    assert info.row_count == 3
    assert info.path.exists()
    assert validate_parquet_file(info.path, expected_row_count=3, allow_empty=False) == 3

    df_chunks = list(iter_parquet_dataframe_chunks(info.path, chunk_size=2))
    assert [len(df) for df in df_chunks] == [2, 1]
    assert df_chunks[0]["year"].dtype == pd.Int64Dtype()
    assert df_chunks[0].loc[0, "year"] == 2024
    assert pd.isna(df_chunks[0].loc[1, "value"])
    assert pd.isna(df_chunks[1].loc[0, "year"])

    assert_raises(ValueError, lambda: list(iter_parquet_dataframe_chunks(info.path, chunk_size=0)))


def test_validate_and_pre_rename_rejection(tmp_path=None):
    target_dir = resolve_dir(tmp_path)

    assert_raises(FileNotFoundError, validate_parquet_file, target_dir / "khong_ton_tai.parquet")

    empty_info = write_records_to_parquet(
        "raw_worldbank_indicators",
        iter([]),
        output_dir=target_dir / "empty_ok",
        chunk_size=2,
        batch_id="b-empty",
        source_endpoint="https://api.worldbank.org/v2",
        ingestion_run_id="run-1",
        allow_empty=True,
    )
    assert read_parquet_file_batch_id(empty_info.path) == "b-empty"
    assert read_parquet_file_batch_id(target_dir / "khong_ton_tai.parquet") is None
    assert (
        validate_parquet_file(
            empty_info.path, expected_row_count=0, allow_empty=True, expected_batch_id="b-empty"
        )
        == 0
    )
    assert_raises(ValueError, validate_parquet_file, empty_info.path, 99, True)
    assert_raises(ValueError, validate_parquet_file, empty_info.path, 0, False)
    assert_raises(ValueError, validate_parquet_file, empty_info.path, 0, True, "sai-batch")

    atomic_dir = target_dir / "atomic_reject"
    assert_raises(
        ValueError,
        write_records_to_parquet,
        "raw_worldbank_countries",
        iter([]),
        output_dir=atomic_dir,
        allow_empty=False,
    )
    assert not (atomic_dir / "raw_worldbank_countries.parquet").exists()
    assert not (atomic_dir / "raw_worldbank_countries.parquet.tmp").exists()

    bad_parquet = target_dir / "thieu_cot_meta.parquet"
    pq.write_table(pa.Table.from_pylist([{"country_iso2": "VN"}]), bad_parquet)
    assert_raises(ValueError, validate_parquet_file, bad_parquet)

    stale_dir = target_dir / "stale_tmp"
    stale_dir.mkdir(parents=True, exist_ok=True)
    stale_file = stale_dir / "a.parquet.tmp"
    keep_file = stale_dir / "b.parquet"
    stale_file.write_text("tam", encoding="utf-8")
    keep_file.write_text("chinh", encoding="utf-8")
    removed = cleanup_stale_temp_parquet_files(stale_dir)
    assert removed == (stale_file,)
    assert not stale_file.exists()
    assert keep_file.exists()


if __name__ == "__main__":
    test_write_and_iter_parquet_chunks()
    test_validate_and_pre_rename_rejection()
    print("PARQUET IO TESTS PASSED")
