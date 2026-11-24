import time
import requests

from src.config.logger import get_logger
from src.config.settings import settings
from src.extract.parser import INDICATORS

logger = get_logger(__name__)

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
RETRYABLE_API_ERROR_IDS = frozenset({"105", "175"})


def extract_api_error(payload):
    if isinstance(payload, list) and len(payload) == 1 and isinstance(payload[0], dict):
        messages = payload[0].get("message")
        if isinstance(messages, list) and messages and isinstance(messages[0], dict):
            first = messages[0]
            error_id = str(first.get("id") or "").strip()
            error_key = str(first.get("key") or "").strip()
            error_val = str(first.get("value") or "").strip()
            detail = " - ".join(part for part in (error_key, error_val) if part) or str(first)
            return error_id, detail
    return None, None


def resolve_retry_wait(attempt, response=None):
    if response is not None and response.status_code == 429 and response.headers:
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                wait_seconds = float(str(retry_after).strip())
                if wait_seconds > 0:
                    return wait_seconds
            except ValueError:
                pass
    return 2 ** max(0, attempt - 1)


def fetch_json_page(session, url, params):
    max_retries = max(1, settings.world_bank_max_retries)
    headers = (
        {"Authorization": f"Bearer {settings.world_bank_api_key}"}
        if settings.world_bank_api_key
        else {}
    )

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(
                url, params=params, headers=headers, timeout=settings.world_bank_timeout
            )
            response.raise_for_status()
            payload = response.json()
            error_id, error_detail = extract_api_error(payload)
            if error_id is not None:
                if error_id in RETRYABLE_API_ERROR_IDS:
                    if attempt == max_retries:
                        raise RuntimeError(
                            f"Lỗi API World Bank tạm thời (id={error_id}) tại {url} (page={params.get('page')}) sau {max_retries} lần thử: {error_detail}"
                        )
                    wait_time = resolve_retry_wait(attempt)
                    logger.warning(
                        "[Thử lại %d/%d] API World Bank trả lỗi tạm thời (id=%s) tại %s (page=%s): %s, chờ %.1fs...",
                        attempt,
                        max_retries,
                        error_id,
                        url,
                        params.get("page"),
                        error_detail,
                        wait_time,
                    )
                    time.sleep(wait_time)
                    continue
                raise RuntimeError(
                    f"Lỗi API World Bank (id={error_id}) tại {url} (page={params.get('page')}): {error_detail}"
                )
            if not isinstance(payload, list) or len(payload) < 2:
                raise ValueError(
                    f"Payload JSON không hợp lệ từ {url} (page={params.get('page')}): {payload!r}"
                )
            meta = payload[0] if isinstance(payload[0], dict) else {}
            rows = payload[1] if isinstance(payload[1], list) else []
            return meta, rows
        except requests.HTTPError as err:
            status = err.response.status_code if err.response is not None else None
            if status not in RETRYABLE_STATUS_CODES or attempt == max_retries:
                raise RuntimeError(
                    f"Lỗi HTTP {status} khi gọi API {url} (page={params.get('page')}): {err}"
                ) from err
            wait_time = resolve_retry_wait(attempt, err.response)
        except (
            requests.Timeout,
            requests.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            ValueError,
        ) as err:
            if attempt == max_retries:
                raise RuntimeError(
                    f"Lỗi khi gọi API {url} (page={params.get('page')}) sau {max_retries} lần thử: {err}"
                ) from err
            wait_time = resolve_retry_wait(attempt)
        except requests.RequestException as err:
            raise RuntimeError(
                f"Lỗi request không thể retry khi gọi API {url} (page={params.get('page')}): {err}"
            ) from err

        logger.warning(
            "[Thử lại %d/%d] Lỗi khi gọi %s (page=%s), chờ %.1fs...",
            attempt,
            max_retries,
            url,
            params.get("page"),
            wait_time,
        )
        time.sleep(wait_time)

    raise RuntimeError(
        f"Không thể lấy dữ liệu từ API {url} (page={params.get('page')}) sau {max_retries} lần thử."
    )


def iter_all_pages(session, url, base_params, label, progress=None, checkpoint_key=""):
    page = 1
    total_pages = 1

    while page <= total_pages:
        params = {
            **base_params,
            "format": "json",
            "per_page": settings.world_bank_per_page,
            "page": page,
        }
        meta, rows = fetch_json_page(session, url, params)
        total_pages = int(meta.get("pages", 0) or 0)

        logger.info("  -> %s | Trang %d/%d: +%d dòng", label, page, total_pages, len(rows))
        for row in rows:
            yield meta, row

        if progress is not None:
            progress["last_page"] = page
            progress["last_key"] = checkpoint_key

        page += 1
        if page <= total_pages:
            time.sleep(settings.world_bank_sleep_seconds)


def fetch_source_indicators(session, source_id=None, max_indicators=None, *, metadata_cache=None):
    resolved_source = str(source_id or settings.world_bank_source_id).strip() or "2"
    resolved_limit = (
        settings.world_bank_max_indicators if max_indicators is None else int(max_indicators)
    )
    if resolved_limit < 0:
        raise ValueError(f"max_indicators phải >= 0, giá trị hiện tại: {resolved_limit}.")

    url = f"{settings.world_bank_api_url}/source/{resolved_source}/indicator"
    discovered = []
    seen = set()
    rows_by_code = {}
    for _, row in iter_all_pages(session, url, {}, f"source:{resolved_source}:indicators"):
        code = (row.get("id") or "").strip()
        if code and code not in seen:
            seen.add(code)
            discovered.append(code)
            rows_by_code[code] = row

    if not discovered:
        raise RuntimeError(f"Không tìm thấy chỉ số nào từ endpoint {url}.")

    if resolved_source == "2":
        priority = [code for code in INDICATORS if code in seen]
        priority_set = set(priority)
        ordered = priority + [code for code in discovered if code not in priority_set]
    else:
        ordered = discovered

    if resolved_limit > 0:
        ordered = ordered[:resolved_limit]

    if metadata_cache is not None:
        for code in ordered:
            metadata_cache[code] = rows_by_code[code]

    logger.info(
        "Đã chọn %d/%d chỉ số từ World Bank source=%s.",
        len(ordered),
        len(discovered),
        resolved_source,
    )
    return tuple(ordered)


def fetch_source_last_updated(session, source_id=None):
    resolved_source = str(source_id or settings.world_bank_source_id).strip() or "2"
    url = f"{settings.world_bank_api_url}/source/{resolved_source}"
    _, rows = fetch_json_page(session, url, {"format": "json"})
    if not rows or not isinstance(rows[0], dict):
        return ""
    return (rows[0].get("lastupdated") or "").strip()
