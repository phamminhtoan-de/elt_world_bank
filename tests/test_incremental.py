from pathlib import Path
import sys

from src.extract.extractor import get_ingestion_tasks
from src.reliability.incremental import plan_incremental_tasks


def test_plan_incremental_tasks():
    task_specs = get_ingestion_tasks(("NY.GDP.MKTP.CD", "FP.CPI.TOTL.ZG"), source_id="2")
    base_watermarks = {
        "raw_worldbank_countries": {"last_loaded_year": None, "api_last_updated": "2026-11-11"},
        "raw_worldbank_indicator_metadata": {"last_loaded_year": None, "api_last_updated": "2026-11-11"},
        "raw_worldbank_indicators__NY.GDP.MKTP.CD": {
            "last_loaded_year": 2025,
            "api_last_updated": "2026-11-11",
        },
        "raw_worldbank_indicators__FP.CPI.TOTL.ZG": {
            "last_loaded_year": 2025,
            "api_last_updated": "2026-11-11",
        },
    }

    unchanged_specs, unchanged_overrides = plan_incremental_tasks(
        task_specs,
        set(),
        base_watermarks,
        date_range="1960:2025",
        current_api_updated="2026-11-11",
        lookback_years=1,
    )
    assert unchanged_specs == ()
    assert unchanged_overrides == {}

    new_year_specs, new_year_overrides = plan_incremental_tasks(
        task_specs,
        set(),
        base_watermarks,
        date_range="1960:2026",
        current_api_updated="2026-11-11",
        lookback_years=3,
        base_load_mode="append",
    )
    assert [spec[0] for spec in new_year_specs] == [
        "raw_worldbank_indicator_metadata",
        "raw_worldbank_indicators__NY.GDP.MKTP.CD",
        "raw_worldbank_indicators__FP.CPI.TOTL.ZG",
    ]
    assert new_year_overrides == {
        "raw_worldbank_indicators__NY.GDP.MKTP.CD": {
            "date_range": "2022:2026",
            "load_mode": "append",
        },
        "raw_worldbank_indicators__FP.CPI.TOTL.ZG": {
            "date_range": "2022:2026",
            "load_mode": "append",
        },
    }

    _, replace_overrides = plan_incremental_tasks(
        task_specs,
        set(),
        base_watermarks,
        date_range="1960:2026",
        current_api_updated="2026-11-11",
        lookback_years=1,
        base_load_mode="replace",
    )
    assert replace_overrides == {
        "raw_worldbank_indicators__NY.GDP.MKTP.CD": {
            "date_range": "2024:2026",
            "load_mode": "upsert",
        },
        "raw_worldbank_indicators__FP.CPI.TOTL.ZG": {
            "date_range": "2024:2026",
            "load_mode": "upsert",
        },
    }

    api_updated_specs, api_updated_overrides = plan_incremental_tasks(
        get_ingestion_tasks(("NY.GDP.MKTP.CD", "SP.POP.TOTL"), source_id="2"),
        set(),
        {
            "raw_worldbank_countries": {"last_loaded_year": None, "api_last_updated": "2026-11-11"},
            "raw_worldbank_indicator_metadata": {"last_loaded_year": None, "api_last_updated": "2026-11-11"},
            "raw_worldbank_indicators__NY.GDP.MKTP.CD": {
                "last_loaded_year": 2025,
                "api_last_updated": "2026-11-11",
            },
        },
        date_range="1960:2025",
        current_api_updated="2026-12-15",
        lookback_years=1,
    )
    assert [spec[0] for spec in api_updated_specs] == [
        "raw_worldbank_countries",
        "raw_worldbank_indicator_metadata",
        "raw_worldbank_indicators__NY.GDP.MKTP.CD",
        "raw_worldbank_indicators__SP.POP.TOTL",
    ]
    assert api_updated_overrides == {
        "raw_worldbank_indicators__NY.GDP.MKTP.CD": {
            "date_range": "2024:2025",
            "load_mode": "append",
        }
    }

    legacy_specs, legacy_overrides = plan_incremental_tasks(
        get_ingestion_tasks(("NY.GDP.MKTP.CD", "SP.POP.TOTL"), source_id="2"),
        {
            "raw_worldbank_countries",
            "raw_worldbank_indicator_metadata",
            "raw_worldbank_indicators__NY.GDP.MKTP.CD",
        },
        {},
        date_range="1960:2025",
        current_api_updated="2026-11-11",
        lookback_years=1,
    )
    assert [spec[0] for spec in legacy_specs] == [
        "raw_worldbank_indicator_metadata",
        "raw_worldbank_indicators__SP.POP.TOTL",
    ]
    assert legacy_overrides == {}


if __name__ == "__main__":
    test_plan_incremental_tasks()
    print("INCREMENTAL TESTS PASSED")
