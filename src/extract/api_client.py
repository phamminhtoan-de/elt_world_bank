import time
import requests

from src.config.logger import get_logger
from src.config.settings import settings
from src.extract.parser import INDICATORS

logger = get_logger(__name__)

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


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
            if not isinstance(payload, list) or len(payload) < 2:
                return {}, []
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
        except (requests.Timeout, requests.ConnectionError) as err:
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

    return {}, []


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


def fetch_source_indicators(session, source_id=None, max_indicators=None):
    resolved_source = str(source_id or settings.world_bank_source_id).strip() or "2"
    resolved_limit = (
        settings.world_bank_max_indicators if max_indicators is None else int(max_indicators)
    )
    if resolved_limit < 0:
        raise ValueError(f"max_indicators phải >= 0, giá trị hiện tại: {resolved_limit}.")

    url = f"{settings.world_bank_api_url}/source/{resolved_source}/indicator"
    discovered = []
    seen = set()
    for _, row in iter_all_pages(session, url, {}, f"source:{resolved_source}:indicators"):
        code = (row.get("id") or "").strip()
        if code and code not in seen:
            seen.add(code)
            discovered.append(code)

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
