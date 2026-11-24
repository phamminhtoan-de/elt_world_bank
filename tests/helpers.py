from contextlib import contextmanager
from pathlib import Path
import tempfile
from unittest.mock import MagicMock


def resolve_dir(tmp_path):
    return Path(tmp_path) if tmp_path is not None else Path(tempfile.mkdtemp())


def assert_raises(expected_exc, func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except expected_exc as err:
        return err
    raise AssertionError(f"Kỳ vọng lỗi {expected_exc.__name__} từ {func.__name__} nhưng không thấy lỗi.")


def make_fake_conn():
    conn = MagicMock()
    conn.executed_queries = []
    conn.copied_queries = []
    conn.transaction_count = 0
    conn.closed = False

    cur = MagicMock()
    cur.execute.side_effect = lambda q, p=None: conn.executed_queries.append((repr(q), p))
    cur.executemany.side_effect = lambda q, seq: conn.executed_queries.extend((repr(q), p) for p in seq)

    @contextmanager
    def fake_copy(q):
        chunks = []
        conn.copied_queries.append((repr(q), chunks))
        writer = MagicMock()
        writer.write.side_effect = chunks.append
        yield writer

    cur.copy.side_effect = fake_copy
    conn.cursor_obj = cur

    @contextmanager
    def fake_transaction():
        conn.transaction_count += 1
        yield

    @contextmanager
    def fake_cursor():
        yield conn.cursor_obj

    conn.transaction.side_effect = fake_transaction
    conn.cursor.side_effect = fake_cursor
    conn.close.side_effect = lambda: setattr(conn, "closed", True)
    return conn


def make_indicator_record(year, value, code="NY.GDP.MKTP.CD", batch_id=""):
    return {
        "country_iso3": "VNM",
        "country_iso2": "VN",
        "country_name": "Viet Nam",
        "indicator_code": code,
        "indicator_name": "Indicator",
        "year": year,
        "value": value,
        "unit": "",
        "obs_status": "",
        "decimal": 0,
        "source_id": "2",
        "last_updated": "2026-11-11",
        "raw_payload": f'{{"countryiso3code":"VNM","indicator":{{"id":"{code}"}},"date":"{year}","value":{value if value is not None else "null"}}}',
        "extracted_at": "2026-11-23T06:00:00+00:00",
        "batch_id": batch_id,
        "source_endpoint": f"https://api.worldbank.org/v2/country/all/indicator/{code}",
        "ingestion_run_id": "run-1",
    }
