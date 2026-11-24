from datetime import datetime, timezone
from pathlib import Path
import time

from src.config.logger import get_logger
from src.metadata.state import finish_run, save_extract_checkpoint
from src.storage.database import open_db_connection
from src.storage.parquet_io import read_parquet_file_batch_id, validate_parquet_file

logger = get_logger("elt.pipeline")


def mark_run_failed(state_schema, ingestion_run_id, error_message):
    if not ingestion_run_id:
        return
    try:
        with open_db_connection() as conn:
            finish_run(
                conn,
                state_schema,
                ingestion_run_id=ingestion_run_id,
                status="FAILED",
                error_message=error_message,
            )
    except Exception as finish_err:
        logger.warning(
            "Không thể cập nhật trạng thái FAILED cho run %s: %s",
            ingestion_run_id,
            finish_err,
        )


def try_recover_extracted_parquet(
    task_key,
    table_name,
    endpoint,
    cp,
    run_data_dir,
    effective_state_schema,
    ingestion_run_id,
    conn=None,
):
    candidate_path = (
        Path(cp["parquet_path"]) if cp.get("parquet_path") else (run_data_dir / f"{task_key}.parquet")
    )
    if not candidate_path.exists():
        return None

    status = cp.get("extract_status")
    expected_batch_id = cp.get("batch_id")
    if status != "EXTRACTED":
        file_batch_id = read_parquet_file_batch_id(candidate_path)
        if expected_batch_id and file_batch_id != expected_batch_id:
            if status == "PENDING" and not cp.get("parquet_path"):
                return None
            logger.warning(
                "[RE-EXTRACT] File Parquet %s có batch_id=%s không khớp (%s); sẽ tải lại từ API.",
                candidate_path,
                file_batch_id,
                expected_batch_id,
            )
            return None

    expected_rows = (
        cp.get("rows_extracted")
        if status in ("EXTRACTED", "FILE_READY") and cp.get("rows_extracted") is not None
        else None
    )
    try:
        recovered_rows = validate_parquet_file(
            candidate_path,
            expected_row_count=expected_rows,
            allow_empty=(table_name == "raw_worldbank_indicators"),
            expected_batch_id=expected_batch_id,
        )
    except Exception as val_err:
        logger.warning(
            "[RE-EXTRACT] File Parquet của %s không hợp lệ (%s); sẽ tải lại từ API.",
            task_key,
            val_err,
        )
        return None

    if status == "EXTRACTED":
        logger.info(
            "[SKIP EXTRACT] %s đã EXTRACTED tại %s (%s dòng)",
            task_key,
            candidate_path,
            f"{recovered_rows:,}",
        )
        return candidate_path

    now = datetime.now(timezone.utc)
    checkpoint_kwargs = {
        "ingestion_run_id": ingestion_run_id,
        "table_name": table_name,
        "batch_id": expected_batch_id,
        "source_endpoint": endpoint,
        "parquet_path": candidate_path,
        "rows_extracted": recovered_rows,
        "last_extracted_page": cp.get("last_extracted_page") or 0,
        "last_checkpoint_key": cp.get("last_checkpoint_key"),
        "extracted_at": now,
        "started_at": now,
        "finished_at": now,
        "duration_seconds": 0.0,
        "status": "EXTRACTED",
        "task_key": task_key,
    }
    if conn is not None and not getattr(conn, "closed", False):
        save_extract_checkpoint(conn, effective_state_schema, **checkpoint_kwargs)
    else:
        with open_db_connection() as active_conn:
            save_extract_checkpoint(active_conn, effective_state_schema, **checkpoint_kwargs)
    cp["extract_status"] = "EXTRACTED"
    cp["parquet_path"] = str(candidate_path)
    cp["rows_extracted"] = recovered_rows
    logger.info(
        "[RECOVER EXTRACT] Khôi phục metadata cho %s từ file Parquet sẵn có %s (%s dòng, batch_id=%s)",
        task_key,
        candidate_path,
        f"{recovered_rows:,}",
        expected_batch_id,
    )
    return candidate_path


def record_extract_metadata_with_retry(
    effective_state_schema,
    *,
    ingestion_run_id,
    task_key,
    table_name,
    batch_id,
    info,
    started_at,
    finished_at,
    duration,
    allow_empty,
    max_attempts=3,
    conn=None,
):
    last_err = None
    checkpoint_kwargs = {
        "ingestion_run_id": ingestion_run_id,
        "table_name": table_name,
        "batch_id": batch_id,
        "source_endpoint": info.source_endpoint,
        "parquet_path": info.path,
        "rows_extracted": info.row_count,
        "last_extracted_page": info.last_extracted_page,
        "last_checkpoint_key": info.last_checkpoint_key,
        "extracted_at": started_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration,
        "task_key": task_key,
    }
    for attempt in range(1, max_attempts + 1):
        try:
            if attempt > 1:
                validate_parquet_file(
                    info.path,
                    expected_row_count=info.row_count,
                    allow_empty=allow_empty,
                    expected_batch_id=batch_id,
                )
            if attempt == 1 and conn is not None and not getattr(conn, "closed", False):
                save_extract_checkpoint(conn, effective_state_schema, status="EXTRACTED", **checkpoint_kwargs)
            else:
                with open_db_connection() as retry_conn:
                    save_extract_checkpoint(retry_conn, effective_state_schema, status="EXTRACTED", **checkpoint_kwargs)
            return
        except Exception as err:
            last_err = err
            if attempt < max_attempts:
                logger.warning(
                    "[RETRY METADATA %d/%d] %s đã ghi Parquet xong (%s) nhưng lỗi lưu checkpoint: %s",
                    attempt,
                    max_attempts,
                    task_key,
                    info.path,
                    err,
                )
                time.sleep(attempt)

    try:
        with open_db_connection() as active_conn:
            save_extract_checkpoint(
                active_conn,
                effective_state_schema,
                status="FILE_READY",
                error_message=str(last_err),
                **checkpoint_kwargs,
            )
    except Exception as state_err:
        logger.warning(
            "[FILE_READY] %s giữ nguyên file Parquet %s (chưa ghi được metadata: %s)",
            task_key,
            info.path,
            state_err,
        )
    raise last_err
