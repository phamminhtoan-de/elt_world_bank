from contextlib import contextmanager
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config.settings import settings

LOGS_DIR = settings.log_dir
LOG_LEVEL_CONSOLE = settings.log_level
LOG_LEVEL_FILE = "DEBUG"


def setup_logging(log_filename="pipeline.log"):
    root = logging.getLogger()
    if root.handlers:
        return root

    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.Formatter.converter = time.gmtime
    root.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, LOG_LEVEL_CONSOLE, logging.INFO))
    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s UTC | %(levelname)-7s | %(module)-9s:%(lineno)-4d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        LOGS_DIR / log_filename, maxBytes=10_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(getattr(logging, LOG_LEVEL_FILE, logging.DEBUG))
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d UTC | %(levelname)-7s | %(name)-14s | %(module)-9s:%(lineno)-4d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(file_handler)

    return root


def get_logger(name=__name__):
    setup_logging()
    return logging.getLogger(name)


@contextmanager
def log_step(logger, step_name):
    start_time = time.perf_counter()
    logger.info("[BẮT ĐẦU] %s", step_name)
    try:
        yield
    except Exception as err:
        elapsed = time.perf_counter() - start_time
        if getattr(err, "_logged_by_step", False):
            logger.error("[THẤT BẠI] %s sau %.2fs: %s", step_name, elapsed, err)
        else:
            setattr(err, "_logged_by_step", True)
            logger.exception("[THẤT BẠI] %s sau %.2fs: %s", step_name, elapsed, err)
        raise
    else:
        elapsed = time.perf_counter() - start_time
        logger.info("[HOÀN TẤT] %s (%.2fs)", step_name, elapsed)
