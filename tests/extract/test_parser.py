import inspect
import json
from pathlib import Path
import sys
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.extract.parser import (
    COUNTRY_COLUMNS,
    INDICATOR_COLUMNS,
    INDICATOR_META_COLUMNS,
    chunked,
    flatten_country_row,
    flatten_indicator_meta_row,
    flatten_indicator_row,
    parse_date_range,
    records_to_dataframe,
)
from tests.helpers import assert_raises


def test_flatten_rows_valid_and_malformed():
    country = flatten_country_row(
        {
            "id": "VNM",
            "iso2Code": "VN",
            "name": "Viet Nam",
            "region": {"id": "EAS", "iso2code": "Z4", "value": "East Asia & Pacific"},
            "adminregion": {"id": "EAP", "value": "East Asia & Pacific"},
            "incomeLevel": {"id": "LMC", "value": "Lower middle income"},
            "lendingType": {"id": "IBD", "value": "IBRD"},
            "capitalCity": "Hanoi",
            "longitude": "105.854",
            "latitude": "21.0285",
        },
        extracted_at="2026-11-23T06:00:00+00:00",
        batch_id="b-1",
        source_endpoint="https://api.worldbank.org/v2/country",
        ingestion_run_id="r-1",
    )
    assert tuple(country.keys()) == COUNTRY_COLUMNS
    assert country["country_iso3"] == "VNM"
    assert country["capital_city"] == "Hanoi"
    assert "raw_payload" in COUNTRY_COLUMNS
    assert json.loads(country["raw_payload"])["id"] == "VNM"

    country_empty = flatten_country_row({})
    assert tuple(country_empty.keys()) == COUNTRY_COLUMNS
    assert country_empty["country_iso3"] == ""
    assert country_empty["region_name"] == ""

    meta = flatten_indicator_meta_row(
        {
            "id": "NY.GDP.MKTP.CD",
            "name": "GDP (current US$)",
            "unit": "",
            "source": {"id": "2", "value": "WDI"},
            "sourceNote": "Note",
            "sourceOrganization": "World Bank",
            "topics": ["chuoi_sai_kieu", {"value": ""}, {"value": "Economy"}, {"value": "Growth"}],
        },
        "FALLBACK.CODE",
    )
    assert tuple(meta.keys()) == INDICATOR_META_COLUMNS
    assert meta["topics"] == "Economy | Growth"
    assert json.loads(meta["raw_payload"])["id"] == "NY.GDP.MKTP.CD"

    meta_bad_topics = flatten_indicator_meta_row({"topics": ["sai_kieu", {"value": ""}]}, "FALLBACK.CODE")
    assert meta_bad_topics["indicator_code"] == "FALLBACK.CODE"
    assert meta_bad_topics["topics"] == ""

    indicator_ok = flatten_indicator_row(
        {
            "country": {"id": "VN", "value": "Viet Nam"},
            "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
            "countryiso3code": "VNM",
            "date": "2024",
            "value": 0.0,
            "decimal": 0,
        },
        {"sourceid": "2", "lastupdated": "2026-11-11"},
        "IND.FALLBACK",
        extracted_at="2026-11-23T06:00:00+00:00",
        batch_id="b-3",
        source_endpoint="https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD",
        ingestion_run_id="r-1",
    )
    assert tuple(indicator_ok.keys()) == INDICATOR_COLUMNS
    assert indicator_ok["year"] == 2024
    assert indicator_ok["value"] == 0.0
    assert json.loads(indicator_ok["raw_payload"])["date"] == "2024"

    indicator_bad = flatten_indicator_row(
        {"country": None, "indicator": None, "date": "", "value": None, "decimal": None},
        {},
        "IND.FALLBACK",
    )
    assert indicator_bad["indicator_code"] == "IND.FALLBACK"
    assert indicator_bad["year"] is None
    assert indicator_bad["value"] is None
    assert indicator_bad["country_iso3"] == ""


def test_chunked_and_records_to_dataframe():
    gen = chunked([1, 2, 3, 4, 5], 2)
    assert inspect.isgenerator(gen)
    assert list(gen) == [[1, 2], [3, 4], [5]]
    assert list(chunked([], 3)) == []
    assert_raises(ValueError, lambda: list(chunked([1, 2], 0)))
    assert_raises(ValueError, lambda: list(chunked([1, 2], -3)))

    df = records_to_dataframe(
        "raw_worldbank_indicators",
        [
            {"country_iso2": "VN", "year": 2024, "value": 100.5, "decimal": 0, "batch_id": "custom-b"},
            {"country_iso2": "US", "year": "nam_sai", "value": "chu_sai", "decimal": "xyz", "batch_id": ""},
        ],
        extracted_at="2026-11-23T06:00:00+00:00",
        batch_id="b-default",
        source_endpoint="https://api.worldbank.org/v2/country/all/indicator",
        ingestion_run_id="r-default",
    )
    assert isinstance(df, pd.DataFrame)
    assert tuple(df.columns) == INDICATOR_COLUMNS
    assert df["year"].dtype == pd.Int64Dtype()
    assert df["decimal"].dtype == pd.Int64Dtype()
    assert df.loc[0, "year"] == 2024
    assert df.loc[0, "batch_id"] == "custom-b"
    assert pd.isna(df.loc[1, "year"])
    assert pd.isna(df.loc[1, "value"])
    assert pd.isna(df.loc[1, "decimal"])
    assert df.loc[1, "batch_id"] == "b-default"
    assert df.loc[1, "ingestion_run_id"] == "r-default"

    assert_raises(ValueError, records_to_dataframe, "bang_khong_ton_tai", [])


def test_parse_date_range():
    assert parse_date_range("1960:2025") == (1960, 2025)
    assert parse_date_range("2024") == (2024, 2024)
    assert_raises(ValueError, parse_date_range, "")
    assert_raises(ValueError, parse_date_range, "2025:2020")
    assert_raises(ValueError, parse_date_range, "abc:2025")
    assert_raises(ValueError, parse_date_range, "2020:2021:2022")


if __name__ == "__main__":
    test_flatten_rows_valid_and_malformed()
    test_chunked_and_records_to_dataframe()
    test_parse_date_range()
    print("PARSER TESTS PASSED")
