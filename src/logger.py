from contextlib import contextmanager
from datetime import datetime
import logging
from pathlib import Path
import sys
import time

try:
    from src.settings import settings
except ImportError:
    from settings import settings

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_log_file_path(log_dir=None):
    target_dir = log_dir or settings.log_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    return target_dir / f"elt_{today}.log"


def _resolve_log_level(level):
    if isinstance(level, int):
        return level
    level_name = (level or settings.log_level).upper()
    resolved = getattr(logging, level_name, None)
    return resolved if isinstance(resolved, int) else logging.INFO


def _configure_stdout_utf8():
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def get_logger(name="elt_world_bank", level=None, log_dir=None):
    resolved_level = _resolve_log_level(level)
    logger = logging.getLogger(name)
    logger.setLevel(resolved_level)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)

    file_path = get_log_file_path(log_dir)
    file_handler = logging.FileHandler(file_path, mode="a", encoding="utf-8")
    file_handler.setLevel(resolved_level)
    file_handler.setFormatter(formatter)

    _configure_stdout_utf8()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(resolved_level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


@contextmanager
def log_step(logger, step_name):
    logger.info("[START] %s", step_name)
    start = time.perf_counter()
    try:
        yield
    except Exception as err:
        elapsed = time.perf_counter() - start
        if getattr(err, "_logged_by_step", False):
            logger.error(
                "[FAIL]  %s (%.2fs) - dừng do lỗi ở bước con: %s",
                step_name,
                elapsed,
                err,
            )
        else:
            logger.exception("[FAIL]  %s (%.2fs) - %s", step_name, elapsed, err)
            try:
                setattr(err, "_logged_by_step", True)
            except (AttributeError, TypeError):
                pass
        raise
    else:
        elapsed = time.perf_counter() - start
        logger.info("[DONE]  %s (%.2fs)", step_name, elapsed)
