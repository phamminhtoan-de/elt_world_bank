from datetime import datetime, timezone
import time

from src.config.logger import get_logger
from src.config.settings import settings
from src.extract.api_client import iter_all_pages
from src.extract.parser import (
    INDICATORS,
    flatten_country_row,
    flatten_indicator_meta_row,
    flatten_indicator_row,
)
from src.storage.parquet_io import write_records_to_parquet

logger = get_logger(__name__)


def get_ingestion_tasks(indicators=None, source_id=None):
    target_indicators = tuple(indicators) if indicators is not None else INDICATORS
    resolved_source = str(source_id or settings.world_bank_source_id).strip() or "2"
    base_url = settings.world_bank_api_url
    tasks = [
        ("raw_worldbank_countries", "raw_worldbank_countries", None, f"{base_url}/country"),
        (
            "raw_worldbank_indicator_metadata",
            "raw_worldbank_indicator_metadata",
            None,
            f"{base_url}/source/{resolved_source}/indicator",
        ),
    ]
    for code in target_indicators:
        tasks.append(
            (
                f"raw_worldbank_indicators__{code}",
                "raw_worldbank_indicators",
                code,
                f"{base_url}/country/all/indicator/{code}",
            )
        )
    return tuple(tasks)


def iter_countries(session, *, batch_id="", ingestion_run_id="", extracted_at=None, progress=None):
    url = f"{settings.world_bank_api_url}/country"
    ts = extracted_at or datetime.now(timezone.utc).isoformat()
    raw_count = 0
    kept_count = 0
    for _, row in iter_all_pages(
        session, url, {}, "countries", progress=progress, checkpoint_key="countries"
    ):
        raw_count += 1
        record = flatten_country_row(
            row,
            extracted_at=ts,
            batch_id=batch_id,
            source_endpoint=url,
            ingestion_run_id=ingestion_run_id,
        )
        if not str(record["country_iso3"]).strip():
            continue
        kept_count += 1
        yield record

    dropped = raw_count - kept_count
    if dropped > 0:
        logger.warning(
            "[DROP INVALID KEY] raw_worldbank_countries: bỏ %d/%d dòng thiếu country_iso3.",
            dropped,
            raw_count,
        )
    if raw_count > 0 and kept_count == 0:
        raise ValueError(
            f"Bảng raw_worldbank_countries nhận {raw_count} dòng từ API nhưng toàn bộ đều thiếu country_iso3."
        )


def iter_indicator_metadata(
    session,
    indicators=INDICATORS,
    *,
    batch_id="",
    ingestion_run_id="",
    extracted_at=None,
    progress=None,
    source_id=None,
    prefetched_metadata=None,
):
    target_codes = list(dict.fromkeys(indicators))
    if not target_codes:
        return

    ts = extracted_at or datetime.now(timezone.utc).isoformat()
    resolved_source = str(source_id or settings.world_bank_source_id).strip() or "2"
    target_set = set(target_codes)
    yielded = set()
    bulk_url = f"{settings.world_bank_api_url}/source/{resolved_source}/indicator"

    if prefetched_metadata:
        for code in target_codes:
            if code in prefetched_metadata and code not in yielded:
                yielded.add(code)
                yield flatten_indicator_meta_row(
                    prefetched_metadata[code],
                    code,
                    extracted_at=ts,
                    batch_id=batch_id,
                    source_endpoint=bulk_url,
                    ingestion_run_id=ingestion_run_id,
                )
        if yielded and progress is not None:
            progress["last_page"] = 1
            progress["last_key"] = f"source:{resolved_source}"

    if len(yielded) < len(target_set):
        for _, row in iter_all_pages(
            session,
            bulk_url,
            {},
            f"metadata:source:{resolved_source}",
            progress=progress,
            checkpoint_key=f"source:{resolved_source}",
        ):
            code = (row.get("id") or "").strip()
            if code in target_set and code not in yielded:
                yielded.add(code)
                yield flatten_indicator_meta_row(
                    row,
                    code,
                    extracted_at=ts,
                    batch_id=batch_id,
                    source_endpoint=bulk_url,
                    ingestion_run_id=ingestion_run_id,
                )

    missing_codes = [code for code in target_codes if code not in yielded]
    for idx, code in enumerate(missing_codes, start=1):
        url = f"{settings.world_bank_api_url}/indicator/{code}"
        for _, row in iter_all_pages(
            session, url, {}, f"metadata:{code}", progress=progress, checkpoint_key=code
        ):
            yield flatten_indicator_meta_row(
                row,
                code,
                extracted_at=ts,
                batch_id=batch_id,
                source_endpoint=url,
                ingestion_run_id=ingestion_run_id,
            )
        if idx < len(missing_codes):
            time.sleep(settings.world_bank_sleep_seconds)


def iter_indicators(
    session,
    indicators=INDICATORS,
    *,
    batch_id="",
    ingestion_run_id="",
    extracted_at=None,
    progress=None,
    date_range=None,
):
    ts = extracted_at or datetime.now(timezone.utc).isoformat()
    effective_date_range = date_range or settings.world_bank_date_range
    for idx, code in enumerate(indicators, start=1):
        url = f"{settings.world_bank_api_url}/country/all/indicator/{code}"
        raw_count = 0
        kept_count = 0
        dropped_invalid_year = 0
        dropped_missing_country = 0
        for meta, row in iter_all_pages(
            session,
            url,
            {"date": effective_date_range},
            f"[{idx}/{len(indicators)}] {code}",
            progress=progress,
            checkpoint_key=code,
        ):
            raw_count += 1
            record = flatten_indicator_row(
                row,
                meta,
                code,
                extracted_at=ts,
                batch_id=batch_id,
                source_endpoint=url,
                ingestion_run_id=ingestion_run_id,
            )
            missing_year = record["year"] is None
            missing_country = not record["country_iso2"]
            missing_indicator = not record["indicator_code"]
            if missing_year or missing_country or missing_indicator:
                if missing_year:
                    dropped_invalid_year += 1
                if missing_country:
                    dropped_missing_country += 1
                continue
            kept_count += 1
            yield record

        dropped_total = raw_count - kept_count
        if dropped_total > 0:
            logger.warning(
                "[DROP INVALID KEY] %s: bỏ %d/%d dòng thiếu khóa (invalid_year=%d, missing_country=%d).",
                code,
                dropped_total,
                raw_count,
                dropped_invalid_year,
                dropped_missing_country,
            )
        if raw_count > 0 and kept_count == 0:
            raise ValueError(
                f"Chỉ số '{code}' nhận {raw_count} dòng từ API nhưng toàn bộ đều thiếu khóa hợp lệ (invalid_year={dropped_invalid_year}, missing_country={dropped_missing_country})."
            )
        if idx < len(indicators):
            time.sleep(settings.world_bank_sleep_seconds)


def extract_task_to_parquet(
    session,
    task_key,
    table_name,
    indicator_code=None,
    *,
    output_dir=None,
    chunk_size=None,
    batch_id="",
    ingestion_run_id="",
    extracted_at=None,
    indicators=INDICATORS,
    source_id=None,
    allow_empty=None,
    date_range=None,
    prefetched_metadata=None,
):
    ts = extracted_at or datetime.now(timezone.utc).isoformat()
    progress = {"last_page": 0, "last_key": ""}
    base = settings.world_bank_api_url
    resolved_source = str(source_id or settings.world_bank_source_id).strip() or "2"

    if table_name == "raw_worldbank_countries":
        endpoint = f"{base}/country"
        records_iter = iter_countries(
            session,
            batch_id=batch_id,
            ingestion_run_id=ingestion_run_id,
            extracted_at=ts,
            progress=progress,
        )
    elif table_name == "raw_worldbank_indicator_metadata":
        endpoint = f"{base}/source/{resolved_source}/indicator"
        records_iter = iter_indicator_metadata(
            session,
            indicators,
            batch_id=batch_id,
            ingestion_run_id=ingestion_run_id,
            extracted_at=ts,
            progress=progress,
            source_id=resolved_source,
            prefetched_metadata=prefetched_metadata,
        )
    elif table_name == "raw_worldbank_indicators":
        targets = (indicator_code,) if indicator_code else indicators
        endpoint = (
            f"{base}/country/all/indicator/{indicator_code}"
            if indicator_code
            else f"{base}/country/all/indicator"
        )
        records_iter = iter_indicators(
            session,
            targets,
            batch_id=batch_id,
            ingestion_run_id=ingestion_run_id,
            extracted_at=ts,
            progress=progress,
            date_range=date_range,
        )
    else:
        raise ValueError(f"Không hỗ trợ bảng '{table_name}'.")

    effective_allow_empty = (
        (table_name == "raw_worldbank_indicators") if allow_empty is None else allow_empty
    )
    info = write_records_to_parquet(
        table_name,
        records_iter,
        output_dir=output_dir,
        chunk_size=chunk_size,
        batch_id=batch_id,
        source_endpoint=endpoint,
        ingestion_run_id=ingestion_run_id,
        extracted_at=ts,
        progress=progress,
        file_stem=task_key,
        allow_empty=effective_allow_empty,
    )
    if info.row_count == 0 and effective_allow_empty:
        logger.warning(
            "Chỉ số %s trả về 0 dòng từ API (%s); vẫn giữ file Parquet rỗng để tiếp tục pipeline.",
            indicator_code or task_key,
            endpoint,
        )
    return info
