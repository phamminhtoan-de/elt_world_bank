import os
from pathlib import Path
import sys
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.config.settings import BASE_DIR, CONFIG_PATH, VALID_LOAD_MODES, load_settings
from tests.helpers import assert_raises, resolve_dir

TEST_ENV = {
    "WORLD_BANK_API_URL": "https://api.worldbank.org/v2",
    "WORLD_BANK_API_KEY": "",
    "WORLD_BANK_DATE_RANGE": "1960:2025",
    "WORLD_BANK_PER_PAGE": "5000",
    "WORLD_BANK_SLEEP_SECONDS": "3",
    "WORLD_BANK_TIMEOUT": "60",
    "WORLD_BANK_MAX_RETRIES": "5",
    "WORLD_BANK_SOURCE_ID": "2",
    "WORLD_BANK_MAX_INDICATORS": "500",
    "TGT_DB_HOST": "localhost",
    "TGT_DB_PORT": "5432",
    "TGT_DB_NAME": "WorldBank_DWH",
    "TGT_DB_USER": "postgres",
    "TGT_DB_PASSWORD": "123",
    "TGT_DB_SCHEMA": "bronze",
    "STATE_DB_SCHEMA": "elt_metadata",
    "LOG_LEVEL": "INFO",
}


def test_load_settings_env_and_pipeline_yaml(tmp_path=None):
    target_dir = resolve_dir(tmp_path)

    with patch.dict(os.environ, TEST_ENV, clear=False):
        default_cfg = load_settings(CONFIG_PATH)

    assert default_cfg.config_path == BASE_DIR / "src" / "pipeline.yml"
    assert default_cfg.world_bank_api_url == "https://api.worldbank.org/v2"
    assert default_cfg.world_bank_per_page == 5000
    assert default_cfg.world_bank_sleep_seconds == 3.0
    assert default_cfg.world_bank_max_indicators == 500
    assert default_cfg.tgt_db_schema == "bronze"
    assert default_cfg.state_db_schema == "elt_metadata"
    assert default_cfg.elt_load_mode in VALID_LOAD_MODES
    assert default_cfg.elt_skip_duplicate_batch is True
    assert default_cfg.elt_chunk_size == 5000
    assert default_cfg.elt_auto_resume is True
    assert default_cfg.elt_fresh is False
    assert default_cfg.elt_backfill_payload is False
    assert default_cfg.elt_incremental_lookback_years == 3
    assert default_cfg.save_raw_parquet is True
    assert default_cfg.elt_run_id is None
    assert default_cfg.raw_data_dir == BASE_DIR / "data"
    assert default_cfg.backfill_raw_payload_enabled is False
    assert default_cfg.backfill_raw_payload_only_null is True
    assert default_cfg.backfill_raw_payload_tables == (
        "raw_worldbank_indicators",
        "raw_worldbank_countries",
        "raw_worldbank_indicator_metadata",
    )
    assert default_cfg.backfill_raw_payload_exclude_meta_columns == (
        "raw_payload",
        "extracted_at",
        "batch_id",
        "source_endpoint",
        "ingestion_run_id",
        "ingested_at",
    )
    assert default_cfg.backfill_parquet_auto_populate_raw_payload is True

    custom_yml = target_dir / "custom_pipeline.yml"
    custom_yml.write_text(
        "\n".join(
            [
                "pipeline:",
                "  load_mode: upsert",
                "  skip_duplicate_batch: false",
                "  chunk_size: 1200",
                "  auto_resume: false",
                "  fresh: true",
                "  save_raw_parquet: false",
                "  run_id: run-override-99",
                "  data_dir: custom_raw",
                "incremental:",
                "  lookback_years: 0",
                "backfill:",
                "  raw_payload:",
                "    enabled: true",
                "    only_null: false",
                "    tables:",
                "      - raw_worldbank_indicators",
                "    exclude_meta_columns:",
                "      - raw_payload",
                "      - batch_id",
                "  parquet_legacy:",
                "    auto_populate_raw_payload: false",
            ]
        ),
        encoding="utf-8",
    )

    with patch.dict(os.environ, TEST_ENV, clear=False):
        custom_cfg = load_settings(custom_yml)

    assert custom_cfg.config_path == custom_yml
    assert custom_cfg.elt_load_mode == "upsert"
    assert custom_cfg.elt_skip_duplicate_batch is False
    assert custom_cfg.elt_chunk_size == 1200
    assert custom_cfg.elt_auto_resume is False
    assert custom_cfg.elt_fresh is True
    assert custom_cfg.elt_backfill_payload is True
    assert custom_cfg.elt_incremental_lookback_years == 0
    assert custom_cfg.save_raw_parquet is False
    assert custom_cfg.elt_run_id == "run-override-99"
    assert custom_cfg.raw_data_dir == BASE_DIR / "custom_raw"
    assert custom_cfg.backfill_raw_payload_enabled is True
    assert custom_cfg.backfill_raw_payload_only_null is False
    assert custom_cfg.backfill_raw_payload_tables == ("raw_worldbank_indicators",)
    assert custom_cfg.backfill_raw_payload_exclude_meta_columns == ("raw_payload", "batch_id")
    assert custom_cfg.backfill_parquet_auto_populate_raw_payload is False


def test_load_settings_validation_errors(tmp_path=None):
    target_dir = resolve_dir(tmp_path)

    bad_mode_yml = target_dir / "bad_mode.yml"
    bad_mode_yml.write_text("pipeline:\n  load_mode: merge_invalid\n", encoding="utf-8")

    bad_bool_yml = target_dir / "bad_bool.yml"
    bad_bool_yml.write_text("pipeline:\n  fresh: invalid_bool\n", encoding="utf-8")

    bad_int_yml = target_dir / "bad_int.yml"
    bad_int_yml.write_text("pipeline:\n  chunk_size: true\n", encoding="utf-8")

    bad_tables_yml = target_dir / "bad_tables.yml"
    bad_tables_yml.write_text(
        "backfill:\n  raw_payload:\n    tables: not_a_list\n", encoding="utf-8"
    )

    with patch.dict(os.environ, TEST_ENV, clear=False):
        assert_raises(FileNotFoundError, load_settings, target_dir / "khong_ton_tai.yml")
        assert_raises(ValueError, load_settings, bad_mode_yml)
        assert_raises(ValueError, load_settings, bad_bool_yml)
        assert_raises(ValueError, load_settings, bad_int_yml)
        assert_raises(ValueError, load_settings, bad_tables_yml)


if __name__ == "__main__":
    test_load_settings_env_and_pipeline_yaml()
    test_load_settings_validation_errors()
    print("SETTINGS TESTS PASSED")
