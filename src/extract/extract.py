from datetime import datetime, timezone
from pathlib import Path
import sys
import time
import requests

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config.logger import get_logger, log_step
from src.config.settings import settings
from src.extract.api_client import fetch_source_indicators, iter_all_pages
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
    for _, row in iter_all_pages(
        session, url, {}, "countries", progress=progress, checkpoint_key="countries"
    ):
        yield flatten_country_row(
            row,
            extracted_at=ts,
            batch_id=batch_id,
            source_endpoint=url,
            ingestion_run_id=ingestion_run_id,
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
):
    target_codes = list(dict.fromkeys(indicators))
    if not target_codes:
        return

    ts = extracted_at or datetime.now(timezone.utc).isoformat()
    resolved_source = str(source_id or settings.world_bank_source_id).strip() or "2"
    target_set = set(target_codes)
    yielded = set()
    bulk_url = f"{settings.world_bank_api_url}/source/{resolved_source}/indicator"

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
        for meta, row in iter_all_pages(
            session,
            url,
            {"date": effective_date_range},
            f"[{idx}/{len(indicators)}] {code}",
            progress=progress,
            checkpoint_key=code,
        ):
            yield flatten_indicator_row(
                row,
                meta,
                code,
                extracted_at=ts,
                batch_id=batch_id,
                source_endpoint=url,
                ingestion_run_id=ingestion_run_id,
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


def extract_all_to_parquet(
    output_dir=None,
    indicators=None,
    chunk_size=None,
    ingestion_run_id="",
    batch_ids=None,
    source_id=None,
    max_indicators=None,
):
    effective_chunk_size = chunk_size if chunk_size is not None else settings.elt_chunk_size
    infos = []

    with requests.Session() as session:
        target_indicators = (
            tuple(indicators)
            if indicators is not None
            else fetch_source_indicators(
                session, source_id=source_id, max_indicators=max_indicators
            )
        )
        for task_key, table_name, indicator_code, endpoint in get_ingestion_tasks(
            target_indicators, source_id=source_id
        ):
            task_batch_id = (batch_ids or {}).get(task_key, "")
            with log_step(logger, f"Extract & ghi Parquet: {task_key} ({endpoint})"):
                info = extract_task_to_parquet(
                    session,
                    task_key,
                    table_name,
                    indicator_code,
                    output_dir=output_dir,
                    chunk_size=effective_chunk_size,
                    batch_id=task_batch_id,
                    ingestion_run_id=ingestion_run_id,
                    indicators=target_indicators,
                    source_id=source_id,
                )
                logger.info("Đã lưu %s dòng vào %s.", f"{info.row_count:,}", info.path)
                infos.append(info)

    return infos


if __name__ == "__main__":
    with log_step(logger, "Chạy độc lập Extract World Bank API -> Parquet (Streaming)"):
        for info in extract_all_to_parquet():
            logger.info("Hoàn tất file Parquet: %s (%s dòng)", info.path, f"{info.row_count:,}")
