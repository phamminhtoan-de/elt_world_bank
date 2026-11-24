from pathlib import Path
import sys
from unittest.mock import MagicMock, patch
from src.extract import extractor as extract_module
from src.extract.extractor import (
    extract_task_to_parquet,
    get_ingestion_tasks,
    iter_countries,
    iter_indicator_metadata,
    iter_indicators,
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

    prefetched = {
        "NY.GDP.MKTP.CD": {"id": "NY.GDP.MKTP.CD", "name": "GDP cached"},
        "SP.POP.TOTL": {"id": "SP.POP.TOTL", "name": "Population cached"},
    }
    with patch.object(extract_module, "iter_all_pages", side_effect=AssertionError("Không được gọi lại API")):
        cached_rows = list(
            iter_indicator_metadata(
                MagicMock(),
                ("NY.GDP.MKTP.CD", "SP.POP.TOTL"),
                source_id="2",
                prefetched_metadata=prefetched,
            )
        )
    assert [(r["indicator_code"], r["indicator_name"]) for r in cached_rows] == [
        ("NY.GDP.MKTP.CD", "GDP cached"),
        ("SP.POP.TOTL", "Population cached"),
    ]


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


def test_iter_drops_invalid_keys_and_raises_when_all_dropped():
    mixed_rows = [
        (
            {"sourceid": "2", "lastupdated": "2026-11-11"},
            {
                "country": {"id": "VN", "value": "Viet Nam"},
                "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
                "countryiso3code": "VNM",
                "date": "2024",
                "value": 100.0,
            },
        ),
        (
            {"sourceid": "2", "lastupdated": "2026-11-11"},
            {
                "country": {"id": "", "value": "World"},
                "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
                "countryiso3code": "WLD",
                "date": "2024",
                "value": 200.0,
            },
        ),
        (
            {"sourceid": "2", "lastupdated": "2026-11-11"},
            {
                "country": {"id": "VN", "value": "Viet Nam"},
                "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
                "countryiso3code": "VNM",
                "date": "2023Q1",
                "value": 50.0,
            },
        ),
        (
            {"sourceid": "2", "lastupdated": "2026-11-11"},
            {
                "country": {"id": "", "value": "Unknown"},
                "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
                "countryiso3code": "",
                "date": "2024",
                "value": 10.0,
            },
        ),
    ]
    with patch.object(extract_module, "iter_all_pages", return_value=iter(mixed_rows)):
        kept = list(iter_indicators(MagicMock(), ("NY.GDP.MKTP.CD",)))
    assert [(r["country_iso2"], r["year"]) for r in kept] == [("VN", 2024), ("WLD", 2024)]

    all_bad_rows = [
        (
            {},
            {
                "country": {"id": "VN", "value": "Viet Nam"},
                "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
                "date": "2023Q1",
                "value": 50.0,
            },
        ),
        (
            {},
            {
                "country": {"id": "", "value": "Unknown"},
                "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
                "countryiso3code": "",
                "date": "2024",
                "value": 10.0,
            },
        ),
    ]
    with patch.object(extract_module, "iter_all_pages", return_value=iter(all_bad_rows)):
        assert_raises(ValueError, lambda: list(iter_indicators(MagicMock(), ("NY.GDP.MKTP.CD",))))

    with patch.object(extract_module, "iter_all_pages", return_value=iter([({}, {"id": ""})])):
        assert_raises(ValueError, lambda: list(iter_countries(MagicMock())))


if __name__ == "__main__":
    test_get_ingestion_tasks_and_bulk_metadata()
    test_extract_task_to_parquet_and_validation()
    test_iter_drops_invalid_keys_and_raises_when_all_dropped()
    print("EXTRACT TESTS PASSED")
