import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
LOG_DIR = BASE_DIR / "logs"
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

VALID_LOAD_MODES = frozenset({"replace", "append"})

load_dotenv(dotenv_path=ENV_PATH)


def _get_required_env(key):
    value = os.getenv(key, "").strip()
    if not value:
        raise ValueError(
            f"Thiếu biến môi trường '{key}'. Hãy kiểm tra file {ENV_PATH}."
        )
    return value


def _get_int_env(key, default=None):
    raw = os.getenv(key, "").strip()
    if not raw:
        if default is None:
            raise ValueError(
                f"Thiếu biến môi trường '{key}'. Hãy kiểm tra file {ENV_PATH}."
            )
        return default
    try:
        return int(raw)
    except ValueError as err:
        raise ValueError(
            f"Biến môi trường '{key}' phải là số nguyên, giá trị hiện tại: '{raw}'."
        ) from err


def _get_float_env(key, default):
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as err:
        raise ValueError(
            f"Biến môi trường '{key}' phải là số thực, giá trị hiện tại: '{raw}'."
        ) from err


def _get_bool_env(key, default):
    raw = os.getenv(key, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"Biến môi trường '{key}' phải là boolean (true/false), giá trị hiện tại: '{raw}'."
    )


def _resolve_world_bank_api_url():
    api_url = os.getenv("WORLD_BANK_API_URL", "").strip()
    if api_url:
        return api_url.rstrip("/")

    legacy_url = os.getenv("WORLD_BANK_BASE_URL", "").strip().rstrip("/")
    if legacy_url:
        suffix = "/country/all/indicator"
        if legacy_url.endswith(suffix):
            return legacy_url[: -len(suffix)].rstrip("/")
        return legacy_url

    raise ValueError(
        f"Thiếu biến môi trường 'WORLD_BANK_API_URL'. Hãy kiểm tra file {ENV_PATH}."
    )


def _get_load_mode():
    mode = os.getenv("ELT_LOAD_MODE", "replace").strip().lower() or "replace"
    if mode not in VALID_LOAD_MODES:
        raise ValueError(
            f"Biến môi trường 'ELT_LOAD_MODE' không hợp lệ: '{mode}'. "
            f"Chỉ chấp nhận: {sorted(VALID_LOAD_MODES)}."
        )
    return mode


class Settings:
    def __init__(
        self,
        world_bank_api_url,
        world_bank_api_key,
        world_bank_date_range,
        world_bank_per_page,
        world_bank_sleep_seconds,
        world_bank_timeout,
        world_bank_max_retries,
        tgt_db_host,
        tgt_db_port,
        tgt_db_name,
        tgt_db_user,
        tgt_db_password,
        tgt_db_schema,
        elt_load_mode,
        save_raw_csv,
        log_level,
        log_dir,
        raw_data_dir,
    ):
        self.world_bank_api_url = world_bank_api_url
        self.world_bank_api_key = world_bank_api_key
        self.world_bank_date_range = world_bank_date_range
        self.world_bank_per_page = world_bank_per_page
        self.world_bank_sleep_seconds = world_bank_sleep_seconds
        self.world_bank_timeout = world_bank_timeout
        self.world_bank_max_retries = world_bank_max_retries
        self.tgt_db_host = tgt_db_host
        self.tgt_db_port = tgt_db_port
        self.tgt_db_name = tgt_db_name
        self.tgt_db_user = tgt_db_user
        self.tgt_db_password = tgt_db_password
        self.tgt_db_schema = tgt_db_schema
        self.elt_load_mode = elt_load_mode
        self.save_raw_csv = save_raw_csv
        self.log_level = log_level
        self.log_dir = log_dir
        self.raw_data_dir = raw_data_dir

    @property
    def db_dsn_safe(self):
        return (
            f"{self.tgt_db_user}@{self.tgt_db_host}:{self.tgt_db_port}"
            f"/{self.tgt_db_name} (schema={self.tgt_db_schema})"
        )


settings = Settings(
    world_bank_api_url=_resolve_world_bank_api_url(),
    world_bank_api_key=os.getenv("WORLD_BANK_API_KEY", "").strip() or None,
    world_bank_date_range=os.getenv("WORLD_BANK_DATE_RANGE", "2000:2024").strip() or "2000:2024",
    world_bank_per_page=_get_int_env("WORLD_BANK_PER_PAGE", 1000),
    world_bank_sleep_seconds=_get_float_env("WORLD_BANK_SLEEP_SECONDS", 0.6),
    world_bank_timeout=_get_int_env("WORLD_BANK_TIMEOUT", 15),
    world_bank_max_retries=_get_int_env("WORLD_BANK_MAX_RETRIES", 3),
    tgt_db_host=_get_required_env("TGT_DB_HOST"),
    tgt_db_port=_get_int_env("TGT_DB_PORT"),
    tgt_db_name=_get_required_env("TGT_DB_NAME"),
    tgt_db_user=_get_required_env("TGT_DB_USER"),
    tgt_db_password=_get_required_env("TGT_DB_PASSWORD"),
    tgt_db_schema=_get_required_env("TGT_DB_SCHEMA"),
    elt_load_mode=_get_load_mode(),
    save_raw_csv=_get_bool_env("SAVE_RAW_CSV", True),
    log_level=(os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"),
    log_dir=LOG_DIR,
    raw_data_dir=RAW_DATA_DIR,
)
