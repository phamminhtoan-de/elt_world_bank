import os
from pathlib import Path
from types import SimpleNamespace
from dotenv import load_dotenv
import yaml

BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / ".env"
CONFIG_PATH = BASE_DIR / "src" / "pipeline.yml"
LOG_DIR = BASE_DIR / "logs"
RAW_DATA_DIR = BASE_DIR / "data"

VALID_LOAD_MODES = frozenset({"replace", "append", "upsert"})

DEFAULT_BACKFILL_TABLES = (
    "raw_worldbank_indicators",
    "raw_worldbank_countries",
    "raw_worldbank_indicator_metadata",
)

DEFAULT_BACKFILL_EXCLUDE_META_COLUMNS = (
    "raw_payload",
    "extracted_at",
    "batch_id",
    "source_endpoint",
    "ingestion_run_id",
    "ingested_at",
)

load_dotenv(dotenv_path=ENV_PATH)


def get_str_env(key, default=""):
    return os.getenv(key, "").strip() or default


def get_required_env(key):
    value = get_str_env(key)
    if not value:
        raise ValueError(f"Thiếu biến môi trường '{key}'. Hãy kiểm tra file {ENV_PATH}.")
    return value


def get_int_env(key, default=None, min_value=None):
    raw = os.getenv(key, "").strip()
    if not raw:
        if default is None:
            raise ValueError(f"Thiếu biến môi trường '{key}'. Hãy kiểm tra file {ENV_PATH}.")
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as err:
            raise ValueError(
                f"Biến môi trường '{key}' phải là số nguyên, giá trị hiện tại: '{raw}'."
            ) from err

    if min_value is not None and value < min_value:
        raise ValueError(
            f"Biến môi trường '{key}' phải >= {min_value}, giá trị hiện tại: {value}."
        )
    return value


def get_float_env(key, default):
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as err:
        raise ValueError(
            f"Biến môi trường '{key}' phải là số thực, giá trị hiện tại: '{raw}'."
        ) from err


def resolve_world_bank_api_url():
    api_url = get_str_env("WORLD_BANK_API_URL")
    if api_url:
        return api_url.rstrip("/")

    legacy_url = get_str_env("WORLD_BANK_BASE_URL").rstrip("/")
    if legacy_url:
        suffix = "/country/all/indicator"
        if legacy_url.endswith(suffix):
            return legacy_url[: -len(suffix)].rstrip("/")
        return legacy_url

    raise ValueError(
        f"Thiếu biến môi trường 'WORLD_BANK_API_URL'. Hãy kiểm tra file {ENV_PATH}."
    )


def load_yaml_config(config_path=CONFIG_PATH):
    path = Path(config_path) if config_path is not None else CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file cấu hình pipeline: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"File cấu hình '{path}' phải là ánh xạ YAML (mapping) ở cấp gốc.")
    return data


def get_yaml_section(config, section_name):
    section = config.get(section_name)
    if section is None:
        return {}
    if not isinstance(section, dict):
        raise ValueError(f"Mục cấu hình '{section_name}' trong YAML phải là mapping.")
    return section


def get_yaml_str(section, key, default=""):
    raw = section.get(key)
    if raw is None:
        return default
    if isinstance(raw, (dict, list, bool)):
        raise ValueError(f"Cấu hình '{key}' trong YAML phải là chuỗi, giá trị hiện tại: {raw!r}.")
    return str(raw).strip() or default


def get_yaml_int(section, key, default, min_value=None):
    raw = section.get(key)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        value = default
    elif isinstance(raw, bool):
        raise ValueError(f"Cấu hình '{key}' trong YAML phải là số nguyên, giá trị hiện tại: {raw!r}.")
    else:
        try:
            value = int(raw)
        except (TypeError, ValueError) as err:
            raise ValueError(
                f"Cấu hình '{key}' trong YAML phải là số nguyên, giá trị hiện tại: {raw!r}."
            ) from err

    if min_value is not None and value < min_value:
        raise ValueError(f"Cấu hình '{key}' phải >= {min_value}, giá trị hiện tại: {value}.")
    return value


def get_yaml_bool(section, key, default):
    raw = section.get(key)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(
        f"Cấu hình '{key}' trong YAML phải là boolean (true/false), giá trị hiện tại: {raw!r}."
    )


def resolve_config_path(raw_path, default_path):
    if not raw_path:
        return default_path
    candidate = Path(raw_path)
    return candidate if candidate.is_absolute() else (BASE_DIR / candidate)


def get_yaml_str_list(section, key, default):
    raw = section.get(key)
    if raw is None:
        return tuple(default)
    if not isinstance(raw, list):
        raise ValueError(f"Cấu hình '{key}' trong YAML phải là danh sách (list).")
    items = []
    for item in raw:
        if item is None or isinstance(item, (dict, list, bool)):
            raise ValueError(
                f"Phần tử trong danh sách '{key}' phải là chuỗi hợp lệ, giá trị hiện tại: {item!r}."
            )
        text = str(item).strip()
        if not text:
            raise ValueError(f"Danh sách '{key}' trong YAML không được chứa phần tử rỗng.")
        items.append(text)
    return tuple(items)


def get_load_mode(pipeline_section):
    mode = get_yaml_str(pipeline_section, "load_mode", "append").lower()
    if mode not in VALID_LOAD_MODES:
        raise ValueError(
            f"Cấu hình 'pipeline.load_mode' không hợp lệ: '{mode}'. "
            f"Chỉ chấp nhận: {sorted(VALID_LOAD_MODES)}."
        )
    return mode


def load_settings(config_path=CONFIG_PATH):
    resolved_config_path = Path(config_path) if config_path is not None else CONFIG_PATH
    config = load_yaml_config(resolved_config_path)
    pipeline_section = get_yaml_section(config, "pipeline")
    incremental_section = get_yaml_section(config, "incremental")
    backfill_section = get_yaml_section(config, "backfill")
    raw_payload_backfill = get_yaml_section(backfill_section, "raw_payload")
    parquet_legacy_backfill = get_yaml_section(backfill_section, "parquet_legacy")

    tgt_db_host = get_required_env("TGT_DB_HOST")
    tgt_db_port = get_int_env("TGT_DB_PORT")
    tgt_db_name = get_required_env("TGT_DB_NAME")
    tgt_db_user = get_required_env("TGT_DB_USER")
    tgt_db_password = get_required_env("TGT_DB_PASSWORD")
    tgt_db_schema = get_required_env("TGT_DB_SCHEMA")
    state_db_schema = get_str_env("STATE_DB_SCHEMA", "elt_metadata")

    backfill_raw_payload_enabled = get_yaml_bool(raw_payload_backfill, "enabled", False)

    return SimpleNamespace(
        config_path=resolved_config_path,
        world_bank_api_url=resolve_world_bank_api_url(),
        world_bank_api_key=get_str_env("WORLD_BANK_API_KEY") or None,
        world_bank_date_range=get_str_env("WORLD_BANK_DATE_RANGE", "1960:2025"),
        world_bank_per_page=get_int_env("WORLD_BANK_PER_PAGE", 10000),
        world_bank_sleep_seconds=get_float_env("WORLD_BANK_SLEEP_SECONDS", 0.3),
        world_bank_timeout=get_int_env("WORLD_BANK_TIMEOUT", 30),
        world_bank_max_retries=get_int_env("WORLD_BANK_MAX_RETRIES", 3),
        world_bank_source_id=get_str_env("WORLD_BANK_SOURCE_ID", "2"),
        world_bank_max_indicators=get_int_env("WORLD_BANK_MAX_INDICATORS", 200, min_value=0),
        tgt_db_host=tgt_db_host,
        tgt_db_port=tgt_db_port,
        tgt_db_name=tgt_db_name,
        tgt_db_user=tgt_db_user,
        tgt_db_password=tgt_db_password,
        tgt_db_schema=tgt_db_schema,
        state_db_schema=state_db_schema,
        elt_load_mode=get_load_mode(pipeline_section),
        elt_skip_duplicate_batch=get_yaml_bool(pipeline_section, "skip_duplicate_batch", True),
        elt_chunk_size=get_yaml_int(pipeline_section, "chunk_size", 5000, min_value=1),
        elt_auto_resume=get_yaml_bool(pipeline_section, "auto_resume", True),
        elt_fresh=get_yaml_bool(pipeline_section, "fresh", False),
        elt_backfill_payload=backfill_raw_payload_enabled,
        elt_incremental_lookback_years=get_yaml_int(
            incremental_section, "lookback_years", 3, min_value=0
        ),
        elt_run_id=get_yaml_str(pipeline_section, "run_id", "") or None,
        save_raw_parquet=get_yaml_bool(pipeline_section, "save_raw_parquet", True),
        backfill_raw_payload_enabled=backfill_raw_payload_enabled,
        backfill_raw_payload_only_null=get_yaml_bool(raw_payload_backfill, "only_null", True),
        backfill_raw_payload_tables=get_yaml_str_list(
            raw_payload_backfill, "tables", DEFAULT_BACKFILL_TABLES
        ),
        backfill_raw_payload_exclude_meta_columns=get_yaml_str_list(
            raw_payload_backfill,
            "exclude_meta_columns",
            DEFAULT_BACKFILL_EXCLUDE_META_COLUMNS,
        ),
        backfill_parquet_auto_populate_raw_payload=get_yaml_bool(
            parquet_legacy_backfill, "auto_populate_raw_payload", True
        ),
        log_level=get_str_env("LOG_LEVEL", "INFO").upper(),
        log_dir=LOG_DIR,
        raw_data_dir=resolve_config_path(
            get_yaml_str(pipeline_section, "data_dir", ""), RAW_DATA_DIR
        ),
        db_dsn_safe=(
            f"{tgt_db_user}@{tgt_db_host}:{tgt_db_port}"
            f"/{tgt_db_name} (schema={tgt_db_schema}, state={state_db_schema})"
        ),
    )


settings = load_settings()
