from pathlib import Path
import sys
from unittest.mock import MagicMock, patch
import requests
from src.extract import api_client as api_module
from src.extract.api_client import (
    fetch_json_page,
    fetch_source_indicators,
    fetch_source_last_updated,
    resolve_retry_wait,
)
from tests.helpers import assert_raises


def test_resolve_retry_wait_and_fetch_json_page():
    assert resolve_retry_wait(1) == 1
    assert resolve_retry_wait(2) == 2
    assert resolve_retry_wait(3) == 4
    assert resolve_retry_wait(1, MagicMock(status_code=429, headers={"Retry-After": "7"})) == 7.0
    assert resolve_retry_wait(2, MagicMock(status_code=429, headers={"Retry-After": "khong_hop_le"})) == 2

    bad_resp = MagicMock(status_code=400, headers={})
    bad_session = MagicMock()
    bad_session.get.return_value = MagicMock(
        raise_for_status=MagicMock(side_effect=requests.HTTPError(response=bad_resp))
    )
    with patch.object(api_module.settings, "world_bank_max_retries", 3):
        assert_raises(RuntimeError, fetch_json_page, bad_session, "https://api.example/v2", {"page": 1})
    assert bad_session.get.call_count == 1

    resp_429 = MagicMock(status_code=429, headers={"Retry-After": "5"})
    resp_503 = MagicMock(status_code=503, headers={})
    ok_resp = MagicMock()
    ok_resp.raise_for_status.return_value = None
    ok_resp.json.return_value = [{"page": 1, "pages": 1}, [{"id": "OK"}]]

    mixed_session = MagicMock()
    mixed_session.get.side_effect = [
        MagicMock(raise_for_status=MagicMock(side_effect=requests.HTTPError(response=resp_429))),
        MagicMock(raise_for_status=MagicMock(side_effect=requests.HTTPError(response=resp_503))),
        requests.Timeout("read timed out"),
        ok_resp,
    ]
    sleep_calls = []
    with (
        patch.object(api_module.settings, "world_bank_max_retries", 4),
        patch.object(api_module.time, "sleep", side_effect=sleep_calls.append),
    ):
        meta, rows = fetch_json_page(mixed_session, "https://api.example/v2", {"page": 1})
    assert meta == {"page": 1, "pages": 1}
    assert rows == [{"id": "OK"}]
    assert sleep_calls == [5.0, 2, 4]

    timeout_session = MagicMock()
    timeout_session.get.side_effect = requests.Timeout("always timeout")
    timeout_sleeps = []
    with (
        patch.object(api_module.settings, "world_bank_max_retries", 3),
        patch.object(api_module.time, "sleep", side_effect=timeout_sleeps.append),
    ):
        assert_raises(RuntimeError, fetch_json_page, timeout_session, "https://api.example/v2", {"page": 1})
    assert timeout_sleeps == [1, 2]


def test_fetch_json_page_fake_200_errors_and_stream_retry():
    perm_resp = MagicMock()
    perm_resp.raise_for_status.return_value = None
    perm_resp.json.return_value = [
        {"message": [{"id": "120", "key": "Invalid value", "value": "Invalid parameter"}]}
    ]
    perm_session = MagicMock()
    perm_session.get.return_value = perm_resp
    perm_sleeps = []
    with (
        patch.object(api_module.settings, "world_bank_max_retries", 3),
        patch.object(api_module.time, "sleep", side_effect=perm_sleeps.append),
    ):
        assert_raises(RuntimeError, fetch_json_page, perm_session, "https://api.example/v2", {"page": 1})
    assert perm_session.get.call_count == 1
    assert perm_sleeps == []

    temp_resp = MagicMock()
    temp_resp.raise_for_status.return_value = None
    temp_resp.json.return_value = [
        {"message": [{"id": "175", "key": "Server busy", "value": "Temporary error"}]}
    ]
    temp_session = MagicMock()
    temp_session.get.return_value = temp_resp
    temp_sleeps = []
    with (
        patch.object(api_module.settings, "world_bank_max_retries", 3),
        patch.object(api_module.time, "sleep", side_effect=temp_sleeps.append),
    ):
        assert_raises(RuntimeError, fetch_json_page, temp_session, "https://api.example/v2", {"page": 1})
    assert temp_session.get.call_count == 3
    assert temp_sleeps == [1, 2]

    bad_json_resp = MagicMock()
    bad_json_resp.raise_for_status.return_value = None
    bad_json_resp.json.side_effect = ValueError("truncated json")
    ok_resp = MagicMock()
    ok_resp.raise_for_status.return_value = None
    ok_resp.json.return_value = [{"page": 1, "pages": 1}, [{"id": "OK"}]]

    stream_session = MagicMock()
    stream_session.get.side_effect = [
        requests.exceptions.ChunkedEncodingError("connection broken"),
        bad_json_resp,
        ok_resp,
    ]
    stream_sleeps = []
    with (
        patch.object(api_module.settings, "world_bank_max_retries", 3),
        patch.object(api_module.time, "sleep", side_effect=stream_sleeps.append),
    ):
        meta, rows = fetch_json_page(stream_session, "https://api.example/v2", {"page": 1})
    assert meta == {"page": 1, "pages": 1}
    assert rows == [{"id": "OK"}]
    assert stream_session.get.call_count == 3
    assert stream_sleeps == [1, 2]

    malformed_resp = MagicMock()
    malformed_resp.raise_for_status.return_value = None
    malformed_resp.json.return_value = [{"unexpected": "shape"}]
    malformed_session = MagicMock()
    malformed_session.get.return_value = malformed_resp
    with (
        patch.object(api_module.settings, "world_bank_max_retries", 2),
        patch.object(api_module.time, "sleep", side_effect=lambda *_: None),
    ):
        assert_raises(RuntimeError, fetch_json_page, malformed_session, "https://api.example/v2", {"page": 1})
    assert malformed_session.get.call_count == 2


def test_fetch_source_indicators_and_last_updated():
    sample_source_rows = [
        ({}, {"id": "AG.CON.FERT.PT.ZS", "name": "Fertilizer consumption"}),
        ({}, {"id": "AG.CON.FERT.PT.ZS", "name": "Duplicate should be skipped"}),
        ({}, {"id": ""}),
        ({}, {"id": "NY.GDP.MKTP.CD", "name": "GDP (current US$)"}),
        ({}, {"id": "CUSTOM.IND.1", "name": "Custom indicator 1"}),
        ({}, {"id": "SP.POP.TOTL", "name": "Population, total"}),
    ]

    with patch.object(api_module, "iter_all_pages", side_effect=lambda *a, **kw: iter(sample_source_rows)):
        assert fetch_source_indicators(MagicMock(), source_id="2", max_indicators=3) == (
            "NY.GDP.MKTP.CD",
            "SP.POP.TOTL",
            "AG.CON.FERT.PT.ZS",
        )
        assert fetch_source_indicators(MagicMock(), source_id="2", max_indicators=0) == (
            "NY.GDP.MKTP.CD",
            "SP.POP.TOTL",
            "AG.CON.FERT.PT.ZS",
            "CUSTOM.IND.1",
        )
        assert fetch_source_indicators(MagicMock(), source_id="99", max_indicators=2) == (
            "AG.CON.FERT.PT.ZS",
            "NY.GDP.MKTP.CD",
        )

    assert_raises(ValueError, fetch_source_indicators, MagicMock(), max_indicators=-1)
    with patch.object(api_module, "iter_all_pages", return_value=iter([])):
        assert_raises(RuntimeError, fetch_source_indicators, MagicMock(), source_id="2", max_indicators=5)

    with patch.object(api_module, "fetch_json_page", return_value=({}, [{"id": "2", "lastupdated": "2026-11-11"}])):
        assert fetch_source_last_updated(MagicMock(), source_id="2") == "2026-11-11"
    with patch.object(api_module, "fetch_json_page", return_value=({}, [])):
        assert fetch_source_last_updated(MagicMock(), source_id="2") == ""


if __name__ == "__main__":
    test_resolve_retry_wait_and_fetch_json_page()
    test_fetch_json_page_fake_200_errors_and_stream_retry()
    test_fetch_source_indicators_and_last_updated()
    print("API CLIENT TESTS PASSED")
