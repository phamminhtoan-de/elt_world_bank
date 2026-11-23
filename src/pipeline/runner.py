from datetime import datetime, timezone
from pathlib import Path
import time
import requests

from src.config.logger import get_logger, log_step
from src.config.settings import VALID_LOAD_MODES, settings
from src.extract.api_client import fetch_source_indicators, fetch_source_last_updated
from src.extract.extract import extract_task_to_parquet, get_ingestion_tasks
from src.extract.parser import parse_date_range
from src.load.load import load_parquet_to_postgres
from src.metadata.incremental import plan_incremental_tasks
from src.metadata.state import (
    create_or_resume_run,
    ensure_state_schema,
    finish_run,
    get_previously_loaded_tasks,
    get_resumable_run_id,
    get_watermarks,
    save_extract_checkpoint,
)
from src.pipeline.recovery import (
    mark_run_failed,
    record_extract_metadata_with_retry,
    try_recover_extracted_parquet,
)
from src.storage.database import backfill_missing_raw_payloads, ensure_schema, open_db_connection
from src.storage.parquet_io import cleanup_stale_temp_parquet_files

logger = get_logger("elt.pipeline")


def run_backfill_payload(*, schema=None, cfg=None):
    active_settings = cfg or settings
    target_schema = schema or active_settings.tgt_db_schema
    with log_step(logger, f"Backfill cột raw_payload JSONB cho schema '{target_schema}'"):
        with open_db_connection() as conn:
            results = backfill_missing_raw_payloads(
                conn,
                target_schema,
                tables=active_settings.backfill_raw_payload_tables,
                exclude_meta_columns=active_settings.backfill_raw_payload_exclude_meta_columns,
                only_null=active_settings.backfill_raw_payload_only_null,
            )
    total_updated = sum(results.values())
    logger.info(
        "Hoàn tất backfill raw_payload tại schema '%s': đã cập nhật %s dòng (%s).",
        target_schema,
        f"{total_updated:,}",
        results,
    )
    return results


def discover_indicators_and_last_updated(indicators, source_id, max_indicators):
    with requests.Session() as session:
        if indicators is not None:
            resolved_indicators = tuple(indicators)
        else:
            with log_step(logger, f"Lấy danh sách chỉ số từ World Bank (source={source_id})"):
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
):
    parquet_paths = {}
    with log_step(logger, "Giai đoạn 1/2: Extract streaming từ API -> Parquet (Checkpoint)"):
        cleanup_stale_temp_parquet_files(run_data_dir)
        with requests.Session() as session:
            for task_key, table_name, indicator_code, endpoint in effective_task_specs:
                cp = checkpoints[task_key]
                recovered_path = try_recover_extracted_parquet(
                    task_key,
                    table_name,
                    endpoint,
                    cp,
                    run_data_dir,
                    effective_state_schema,
                    ingestion_run_id,
                )
                if recovered_path is not None:
                    parquet_paths[task_key] = recovered_path
                    continue

                started_at = datetime.now(timezone.utc)
                start_perf = time.perf_counter()
                task_date_range = task_overrides.get(task_key, {}).get("date_range")
                try:
                    with log_step(logger, f"Extract & ghi Parquet: {task_key}"):
                        info = extract_task_to_parquet(
                            session,
                            task_key,
                            table_name,
                            indicator_code,
                            output_dir=run_data_dir,
                            chunk_size=effective_chunk_size,
                            batch_id=cp["batch_id"],
                            ingestion_run_id=ingestion_run_id,
                            extracted_at=started_at.isoformat(),
                            indicators=resolved_indicators,
                            source_id=effective_source_id,
                            date_range=task_date_range,
                        )
                except Exception as err:
                    finished_at = datetime.now(timezone.utc)
                    duration = time.perf_counter() - start_perf
                    try:
                        with open_db_connection() as conn:
                            save_extract_checkpoint(
                                conn,
                                effective_state_schema,
                                ingestion_run_id=ingestion_run_id,
                                table_name=table_name,
                                batch_id=cp["batch_id"],
                                source_endpoint=endpoint,
                                parquet_path=None,
                                rows_extracted=0,
                                last_extracted_page=0,
                                last_checkpoint_key=None,
                                extracted_at=started_at,
                                started_at=started_at,
                                finished_at=finished_at,
                                duration_seconds=duration,
                                status="FAILED",
                                error_message=str(err),
                                task_key=task_key,
                            )
                    except Exception as state_err:
                        logger.warning(
                            "Không thể lưu trạng thái lỗi EXTRACT cho %s: %s",
                            task_key,
                            state_err,
                        )
                    raise

                finished_at = datetime.now(timezone.utc)
                duration = time.perf_counter() - start_perf
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
                )
                parquet_paths[task_key] = info.path
                logger.info(
                    "  -> Đã lưu checkpoint EXTRACT cho %s (%s dòng, page=%s)",
                    task_key,
                    f"{info.row_count:,}",
                    info.last_extracted_page,
                )
    return parquet_paths


def run_load_stage(
    effective_task_specs,
    checkpoints,
    parquet_paths,
    task_overrides,
    *,
    target_schema,
    effective_state_schema,
    effective_mode,
    effective_chunk_size,
    config_end_year,
    current_api_updated,
    ingestion_run_id,
    active_settings,
):
    results = {}
    with log_step(logger, "Giai đoạn 2/2: Load streaming từ Parquet -> PostgreSQL Bronze (Atomic)"):
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    ensure_schema(cur, target_schema)

            for task_key, table_name, indicator_code, _ in effective_task_specs:
                cp = checkpoints[task_key]
                if cp.get("load_status") == "LOADED":
                    loaded_rows = int(cp.get("rows_loaded") or 0)
                    logger.info(
                        "[SKIP LOAD] %s đã LOADED (%s dòng)",
                        task_key,
                        f"{loaded_rows:,}",
                    )
                    results[table_name] = results.get(table_name, 0) + loaded_rows
                    continue

                parquet_path = parquet_paths[task_key]
                task_mode = task_overrides.get(task_key, {}).get("load_mode", effective_mode)
                task_wm_year = config_end_year if table_name == "raw_worldbank_indicators" else None
                with log_step(
                    logger,
                    f"Load atomic {parquet_path.name} -> {target_schema}.{table_name}",
                ):
                    rows_loaded = load_parquet_to_postgres(
                        conn,
                        parquet_path,
                        batch_id=cp["batch_id"],
                        schema=target_schema,
                        load_mode=task_mode,
                        table_name=table_name,
                        chunk_size=effective_chunk_size,
                        state_schema=effective_state_schema,
                        ingestion_run_id=ingestion_run_id,
                        task_key=task_key,
                        indicator_code=indicator_code,
                        watermark_year=task_wm_year,
                        api_last_updated=current_api_updated,
                        auto_populate_raw_payload=active_settings.backfill_parquet_auto_populate_raw_payload,
                        skip_duplicate_batch=active_settings.elt_skip_duplicate_batch,
                    )
                results[table_name] = results.get(table_name, 0) + rows_loaded

            finish_run(
                conn,
                effective_state_schema,
                ingestion_run_id=ingestion_run_id,
                status="SUCCESS",
            )
    return results


def run_ingestion(
    *,
    data_dir=None,
    load_mode=None,
    schema=None,
    state_schema=None,
    chunk_size=None,
    auto_resume=None,
    fresh=None,
    run_id=None,
    indicators=None,
    source_id=None,
    max_indicators=None,
    cfg=None,
):
    active_settings = cfg or settings
    base_data_dir = Path(data_dir) if data_dir is not None else active_settings.raw_data_dir
    effective_mode = (load_mode or active_settings.elt_load_mode).lower()
    if effective_mode not in VALID_LOAD_MODES:
        raise ValueError(
            f"load_mode không hợp lệ: '{effective_mode}'. Chỉ chấp nhận {sorted(VALID_LOAD_MODES)}."
        )

    target_schema = schema or active_settings.tgt_db_schema
    effective_state_schema = state_schema or active_settings.state_db_schema
    effective_chunk_size = chunk_size if chunk_size is not None else active_settings.elt_chunk_size
    effective_fresh = active_settings.elt_fresh if fresh is None else fresh
    effective_auto_resume = (
        (False if effective_fresh else active_settings.elt_auto_resume)
        if auto_resume is None
        else auto_resume
    )
    effective_run_id = run_id if run_id is not None else active_settings.elt_run_id
    effective_source_id = str(source_id or active_settings.world_bank_source_id).strip() or "2"
    effective_max_indicators = (
        active_settings.world_bank_max_indicators if max_indicators is None else max_indicators
    )
    if effective_max_indicators < 0:
        raise ValueError(
            f"max_indicators phải >= 0, giá trị hiện tại: {effective_max_indicators}."
        )

    _, config_end_year = parse_date_range(active_settings.world_bank_date_range)
    resolved_indicators, current_api_updated = discover_indicators_and_last_updated(
        indicators, effective_source_id, effective_max_indicators
    )

    task_specs = get_ingestion_tasks(resolved_indicators, source_id=effective_source_id)
    effective_task_specs = task_specs
    task_overrides = {}

    with log_step(logger, f"Khởi tạo state schema '{effective_state_schema}'"):
        with open_db_connection() as conn:
            ensure_state_schema(conn, effective_state_schema)
            if effective_auto_resume:
                resumable_run_id = get_resumable_run_id(
                    conn, effective_state_schema, run_id=effective_run_id
                )
                loaded_tasks = get_previously_loaded_tasks(
                    conn,
                    effective_state_schema,
                    target_schema,
                    exclude_run_id=resumable_run_id,
                )
                watermarks = get_watermarks(
                    conn,
                    effective_state_schema,
                    target_schema,
                    exclude_run_id=resumable_run_id,
                )
                if loaded_tasks or watermarks:
                    effective_task_specs, task_overrides = plan_incremental_tasks(
                        task_specs,
                        loaded_tasks,
                        watermarks,
                        date_range=active_settings.world_bank_date_range,
                        current_api_updated=current_api_updated,
                        lookback_years=active_settings.elt_incremental_lookback_years,
                        base_load_mode=effective_mode,
                    )
                    if not effective_task_specs:
                        logger.info(
                            "Tất cả %d task (%d chỉ số) đều đã cập nhật đến năm %s (API lastupdated=%s) tại schema '%s'.",
                            len(task_specs),
                            len(resolved_indicators),
                            config_end_year,
                            current_api_updated or "n/a",
                            target_schema,
                        )
                        return {}
                    if len(effective_task_specs) < len(task_specs) or task_overrides:
                        inc_mode = "append" if effective_mode == "append" else "upsert"
                        logger.info(
                            "Incremental plan: chạy %d/%d task (bỏ qua %d task không có dữ liệu mới, %d task dùng cửa sổ incremental + %s).",
                            len(effective_task_specs),
                            len(task_specs),
                            len(task_specs) - len(effective_task_specs),
                            len(task_overrides),
                            inc_mode,
                        )

            ingestion_run_id, resumed, checkpoints = create_or_resume_run(
                conn,
                effective_state_schema,
                table_specs=effective_task_specs,
                load_mode=effective_mode,
                target_schema=target_schema,
                data_dir=base_data_dir,
                chunk_size=effective_chunk_size,
                auto_resume=effective_auto_resume,
                run_id=effective_run_id,
            )

    first_cp = next(iter(checkpoints.values()), {})
    run_data_dir = Path(first_cp.get("run_data_dir") or base_data_dir)

    logger.info(
        "Cấu hình ELT Ingestion | ingestion_run_id=%s | resumed=%s | target=%s | state_schema=%s | load_mode=%s | chunk_size=%s | indicators=%d | tasks=%d | source_id=%s | api_lastupdated=%s | run_data_dir=%s",
        ingestion_run_id,
        resumed,
        active_settings.db_dsn_safe,
        effective_state_schema,
        effective_mode,
        effective_chunk_size,
        len(resolved_indicators),
        len(effective_task_specs),
        effective_source_id,
        current_api_updated or "n/a",
        run_data_dir,
    )

    try:
        with log_step(logger, f"ELT Ingestion Pipeline (ingestion_run_id={ingestion_run_id})"):
            parquet_paths = run_extract_stage(
                effective_task_specs,
                checkpoints,
                task_overrides,
                run_data_dir=run_data_dir,
                effective_chunk_size=effective_chunk_size,
                effective_state_schema=effective_state_schema,
                ingestion_run_id=ingestion_run_id,
                resolved_indicators=resolved_indicators,
                effective_source_id=effective_source_id,
            )
            results = run_load_stage(
                effective_task_specs,
                checkpoints,
                parquet_paths,
                task_overrides,
                target_schema=target_schema,
                effective_state_schema=effective_state_schema,
                effective_mode=effective_mode,
                effective_chunk_size=effective_chunk_size,
                config_end_year=config_end_year,
                current_api_updated=current_api_updated,
                ingestion_run_id=ingestion_run_id,
                active_settings=active_settings,
            )
            total_rows = sum(results.values())
            logger.info(
                "Tổng kết ELT Ingestion (ingestion_run_id=%s): đã xử lý %s bảng, tổng cộng %s dòng.",
                ingestion_run_id,
                len(results),
                f"{total_rows:,}",
            )
            return results
    except Exception as err:
        mark_run_failed(effective_state_schema, ingestion_run_id, str(err))
        raise
