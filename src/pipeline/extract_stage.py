from contextlib import ExitStack
from datetime import datetime, timezone
import time
import requests

from src.config.logger import get_logger, log_step
from src.extract.api_client import fetch_source_indicators, fetch_source_last_updated
from src.extract.extractor import extract_task_to_parquet
from src.reliability.recovery import (
    record_extract_metadata_with_retry,
    reset_db_conn,
    save_extract_checkpoint_resilient,
    try_recover_extracted_parquet,
)
from src.storage.database import is_conn_unusable, is_transient_db_error, open_db_connection
from src.storage.parquet_io import cleanup_stale_temp_parquet_files

logger = get_logger("elt.pipeline")


def discover_indicators_and_last_updated(
    indicators,
    source_id,
    max_indicators,
    *,
    metadata_cache=None,
):
    with requests.Session() as session:
        if indicators is not None:
            resolved_indicators = tuple(indicators)
        else:
            with log_step(logger, f"Lấy danh sách chỉ số từ World Bank (source={source_id})"):
                try:
                    resolved_indicators = fetch_source_indicators(
                        session,
                        source_id=source_id,
                        max_indicators=max_indicators,
                        metadata_cache=metadata_cache,
                    )
                except TypeError:
                    resolved_indicators = fetch_source_indicators(
                        session, source_id=source_id, max_indicators=max_indicators
                    )

        try:
            current_api_updated = fetch_source_last_updated(session, source_id=source_id)
        except Exception as err:
            logger.warning(
                "Không thể kiểm tra lastupdated của source=%s (%s); bỏ qua kiểm tra API update.",
                source_id,
                err,
            )
            current_api_updated = ""
    return resolved_indicators, current_api_updated


def run_extract_stage(
    effective_task_specs,
    checkpoints,
    task_overrides,
    *,
    run_data_dir,
    effective_chunk_size,
    effective_state_schema,
    ingestion_run_id,
    resolved_indicators,
    effective_source_id,
    prefetched_metadata=None,
    db_max_retries=3,
):
    parquet_paths = {}
    extract_failures = []
    with log_step(logger, "Giai đoạn 1/2: Extract streaming từ API -> Parquet (Checkpoint)"):
        cleanup_stale_temp_parquet_files(run_data_dir)
        with requests.Session() as session, ExitStack() as db_stack:
            state_conn = db_stack.enter_context(open_db_connection())
            for task_key, table_name, indicator_code, endpoint in effective_task_specs:
                cp = checkpoints[task_key]
                if cp.get("load_status") == "LOADED":
                    logger.info("[SKIP EXTRACT] %s đã LOADED; bỏ qua extract.", task_key)
                    continue

                if is_conn_unusable(state_conn):
                    state_conn = reset_db_conn(db_stack, open_conn_fn=open_db_connection)

                recovered_path = try_recover_extracted_parquet(
                    task_key,
                    table_name,
                    endpoint,
                    cp,
                    run_data_dir,
                    effective_state_schema,
                    ingestion_run_id,
                    conn=state_conn,
                )
                if recovered_path is not None:
                    parquet_paths[task_key] = recovered_path
                    continue

                started_at = datetime.now(timezone.utc)
                start_perf = time.perf_counter()
                task_date_range = task_overrides.get(task_key, {}).get("date_range")
                try:
                    with log_step(logger, f"Extract & ghi Parquet: {task_key}"):
                        extract_kwargs = {
                            "output_dir": run_data_dir,
                            "chunk_size": effective_chunk_size,
                            "batch_id": cp["batch_id"],
                            "ingestion_run_id": ingestion_run_id,
                            "extracted_at": started_at.isoformat(),
                            "indicators": resolved_indicators,
                            "source_id": effective_source_id,
                            "date_range": task_date_range,
                        }
                        if table_name == "raw_worldbank_indicator_metadata" and prefetched_metadata:
                            extract_kwargs["prefetched_metadata"] = prefetched_metadata
                        info = extract_task_to_parquet(
                            session,
                            task_key,
                            table_name,
                            indicator_code,
                            **extract_kwargs,
                        )
                except Exception as err:
                    finished_at = datetime.now(timezone.utc)
                    duration = time.perf_counter() - start_perf
                    cp["extract_status"] = "FAILED"
                    fail_kwargs = {
                        "ingestion_run_id": ingestion_run_id,
                        "table_name": table_name,
                        "batch_id": cp["batch_id"],
                        "source_endpoint": endpoint,
                        "parquet_path": None,
                        "rows_extracted": 0,
                        "last_extracted_page": 0,
                        "last_checkpoint_key": None,
                        "extracted_at": started_at,
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "duration_seconds": duration,
                        "status": "FAILED",
                        "error_message": str(err),
                        "task_key": task_key,
                    }
                    try:
                        save_extract_checkpoint_resilient(
                            effective_state_schema,
                            conn=state_conn,
                            open_conn_fn=open_db_connection,
                            max_attempts=db_max_retries,
                            **fail_kwargs,
                        )
                    except Exception as state_err:
                        db_stack.close()
                        state_conn = None
                        logger.warning(
                            "Không thể lưu trạng thái lỗi EXTRACT cho %s: %s",
                            task_key,
                            state_err,
                        )
                    if table_name != "raw_worldbank_indicators" or is_transient_db_error(err):
                        raise
                    extract_failures.append((task_key, str(err)))
                    logger.error(
                        "[ISOLATE EXTRACT] Bỏ qua chỉ số lỗi %s để chạy tiếp các chỉ số khác: %s",
                        task_key,
                        err,
                    )
                    continue

                finished_at = datetime.now(timezone.utc)
                duration = time.perf_counter() - start_perf
                try:
                    record_extract_metadata_with_retry(
                        effective_state_schema,
                        ingestion_run_id=ingestion_run_id,
                        task_key=task_key,
                        table_name=table_name,
                        batch_id=cp["batch_id"],
                        info=info,
                        started_at=started_at,
                        finished_at=finished_at,
                        duration=duration,
                        allow_empty=(table_name == "raw_worldbank_indicators"),
                        max_attempts=max(1, db_max_retries),
                        conn=state_conn,
                    )
                except Exception as meta_err:
                    cp["extract_status"] = "FILE_READY"
                    cp["parquet_path"] = str(info.path)
                    if table_name != "raw_worldbank_indicators" or is_transient_db_error(meta_err):
                        raise
                    extract_failures.append((task_key, str(meta_err)))
                    logger.error(
                        "[ISOLATE EXTRACT] Lỗi lưu checkpoint cho %s (giữ file Parquet): %s",
                        task_key,
                        meta_err,
                    )
                    continue

                cp["extract_status"] = "EXTRACTED"
                cp["parquet_path"] = str(info.path)
                parquet_paths[task_key] = info.path
                logger.info(
                    "  -> Đã lưu checkpoint EXTRACT cho %s (%s dòng, page=%s)",
                    task_key,
                    f"{info.row_count:,}",
                    info.last_extracted_page,
                )
    return parquet_paths, extract_failures
