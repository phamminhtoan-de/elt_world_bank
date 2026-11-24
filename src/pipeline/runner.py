from pathlib import Path

from src.config.logger import get_logger, log_step
from src.config.settings import VALID_LOAD_MODES, settings
from src.extract.extractor import get_ingestion_tasks
from src.extract.parser import parse_date_range
from src.pipeline.extract_stage import discover_indicators_and_last_updated, run_extract_stage
from src.pipeline.load_only import run_load_only
from src.pipeline.load_stage import run_load_stage
from src.reliability.incremental import plan_incremental_tasks
from src.reliability.recovery import build_failure_summary, mark_run_failed
from src.reliability.runs import create_or_resume_run, get_resumable_run_id
from src.reliability.schema import ensure_state_schema
from src.reliability.watermarks import get_previously_loaded_tasks, get_watermarks
from src.storage.database import backfill_missing_raw_payloads, open_db_connection

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
    load_only=None,
    cfg=None,
):
    active_settings = cfg or settings
    effective_load_only = (
        getattr(active_settings, "elt_load_only", False) if load_only is None else load_only
    )
    if effective_load_only:
        return run_load_only(
            data_dir=data_dir,
            load_mode=load_mode,
            schema=schema,
            state_schema=state_schema,
            chunk_size=chunk_size,
            auto_resume=auto_resume,
            fresh=fresh,
            run_id=run_id,
            indicators=indicators,
            cfg=active_settings,
        )

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
    prefetched_metadata = {}
    resolved_indicators, current_api_updated = discover_indicators_and_last_updated(
        indicators,
        effective_source_id,
        effective_max_indicators,
        metadata_cache=prefetched_metadata,
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
            parquet_paths, extract_failures = run_extract_stage(
                effective_task_specs,
                checkpoints,
                task_overrides,
                run_data_dir=run_data_dir,
                effective_chunk_size=effective_chunk_size,
                effective_state_schema=effective_state_schema,
                ingestion_run_id=ingestion_run_id,
                resolved_indicators=resolved_indicators,
                effective_source_id=effective_source_id,
                prefetched_metadata=prefetched_metadata,
                db_max_retries=getattr(active_settings, "db_max_retries", 3),
            )
            results, load_failures = run_load_stage(
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
                has_extract_failures=bool(extract_failures),
            )
            all_failures = [*extract_failures, *load_failures]
            total_rows = sum(results.values())
            logger.info(
                "Tổng kết ELT Ingestion (ingestion_run_id=%s): đã xử lý %s bảng, tổng cộng %s dòng.",
                ingestion_run_id,
                len(results),
                f"{total_rows:,}",
            )
            if all_failures:
                raise RuntimeError(build_failure_summary(all_failures))
            return results
    except Exception as err:
        mark_run_failed(effective_state_schema, ingestion_run_id, str(err))
        raise
