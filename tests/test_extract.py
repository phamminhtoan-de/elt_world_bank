from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

from src.extract import extract as extract_module
from src.extract.extract import (
    extract_task_to_parquet,
    get_ingestion_tasks,
    iter_indicator_metadata,
)
from tests.helpers import assert_raises, resolve_dir


def test_get_ingestion_tasks_and_bulk_metadata():
    tasks = get_ingestion_tasks(("NY.GDP.MKTP.CD", "FP.CPI.TOTL.ZG"), source_id="2")
    assert len(tasks) == 4
    assert tasks[0][0] == "raw_worldbank_countries"
    assert tasks[1][0] == "raw_worldbank_indicator_metadata"
    assert tasks[1][3].endswith("/source/2/indicator")
    assert tasks[2][0] == "raw_worldbank_indicators__NY.GDP.MKTP.CD"
    assert tasks[3][0] == "raw_worldbank_indicators__FP.CPI.TOTL.ZG"

    sample_bulk_rows = [
        ({}, {"id": "NY.GDP.MKTP.CD", "name": "GDP"}),
        ({}, {"id": "SP.POP.TOTL", "name": "Population"}),
    ]

    def fake_iter_all_pages(session, url, base_params, label, progress=None, checkpoint_key=""):
        if url.endswith("/source/2/indicator"):
            return iter(sample_bulk_rows)
        if url.endswith("/indicator/EXTRA.CODE"):
            return iter([({}, {"id": "EXTRA.CODE", "name": "Extra indicator"})])
        return iter([])

    with patch.object(extract_module, "iter_all_pages", side_effect=fake_iter_all_pages):
        meta_rows = list(
            iter_indicator_metadata(
                MagicMock(),
                ("NY.GDP.MKTP.CD", "SP.POP.TOTL", "EXTRA.CODE", "NY.GDP.MKTP.CD"),
                source_id="2",
            )
        )
    assert [row["indicator_code"] for row in meta_rows] == [
        "NY.GDP.MKTP.CD",
        "SP.POP.TOTL",
        "EXTRA.CODE",
    ]
    assert list(iter_indicator_metadata(MagicMock(), ())) == []


def test_extract_task_to_parquet_and_validation(tmp_path=None):
    target_dir = resolve_dir(tmp_path)
    seen_calls = []

    def fake_iter_all_pages(session, url, base_params, label, progress=None, checkpoint_key=""):
        seen_calls.append((url, dict(base_params)))
        if progress is not None:
            progress["last_page"] = 1
            progress["last_key"] = checkpoint_key
        if url.endswith("/country"):
            return iter([({}, {"id": "VNM", "iso2Code": "VN", "name": "Viet Nam"})])
        if "/country/all/indicator/" in url:
            if url.endswith("/EMPTY.IND"):
                return iter([])
            return iter(
                [
                    (
                        {"sourceid": "2", "lastupdated": "2026-11-11"},
                        {
                            "country": {"id": "VN", "value": "Viet Nam"},
                            "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
                            "countryiso3code": "VNM",
                            "date": "2024",
                            "value": 476.0,
                            "decimal": 0,
                        },
                    )
                ]
            )
        return iter([])

    with patch.object(extract_module, "iter_all_pages", side_effect=fake_iter_all_pages):
        country_info = extract_task_to_parquet(
            MagicMock(),
            "raw_worldbank_countries",
            "raw_worldbank_countries",
            output_dir=target_dir,
            batch_id="b-1",
            ingestion_run_id="r-1",
        )
        assert country_info.row_count == 1
        assert country_info.path.exists()

        ind_info = extract_task_to_parquet(
            MagicMock(),
            "raw_worldbank_indicators__NY.GDP.MKTP.CD",
            "raw_worldbank_indicators",
            "NY.GDP.MKTP.CD",
            output_dir=target_dir,
            batch_id="b-2",
            ingestion_run_id="r-1",
            date_range="2020:2024",
        )
        assert ind_info.row_count == 1
        assert seen_calls[-1][1] == {"date": "2020:2024"}

        empty_ind = extract_task_to_parquet(
            MagicMock(),
            "raw_worldbank_indicators__EMPTY.IND",
            "raw_worldbank_indicators",
            "EMPTY.IND",
            output_dir=target_dir,
            batch_id="b-3",
            ingestion_run_id="r-1",
        )
        assert empty_ind.row_count == 0
        assert empty_ind.path.exists()

    with patch.object(extract_module, "iter_all_pages", return_value=iter([])):
        assert_raises(
            ValueError,
            extract_task_to_parquet,
            MagicMock(),
            "raw_worldbank_countries",
            "raw_worldbank_countries",
            output_dir=target_dir / "empty_country",
            batch_id="b-err",
            ingestion_run_id="r-1",
        )

    assert_raises(
        ValueError,
        extract_task_to_parquet,
        MagicMock(),
        "bang_sai",
        "bang_sai",
        output_dir=target_dir,
    )


if __name__ == "__main__":
    test_get_ingestion_tasks_and_bulk_metadata()
    test_extract_task_to_parquet_and_validation()
    print("EXTRACT TESTS PASSED")
