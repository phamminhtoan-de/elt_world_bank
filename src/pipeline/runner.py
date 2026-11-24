from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
import time
from psycopg import sql
import requests

from src.config.logger import get_logger, log_step
from src.config.settings import VALID_LOAD_MODES, settings
from src.extract.api_client import fetch_source_indicators, fetch_source_last_updated
from src.extract.extract import extract_task_to_parquet, get_ingestion_tasks
from src.extract.parser import parse_date_range
from src.load.load import discover_parquet_files, load_parquet_to_postgres
from src.metadata.incremental import plan_incremental_tasks
from src.metadata.schema import CHECKPOINTS_TABLE
from src.metadata.state import (
    create_or_resume_run,
    ensure_state_schema,
    finish_run,
    get_previously_loaded_tasks,
    get_resumable_run_id,
    get_watermarks,
    record_load_failure,
    save_extract_checkpoint,
)
from src.pipeline.recovery import (
    mark_run_failed,
    record_extract_metadata_with_retry,
    save_extract_checkpoint_resilient,
    try_recover_extracted_parquet,
)
from src.storage.database import (
    backfill_missing_raw_payloads,
    ensure_raw_table,
    ensure_schema,
    is_conn_unusable,
    is_transient_db_error,
    open_db_connection,
    table_ref,
)
from src.storage.parquet_io import cleanup_stale_temp_parquet_files, read_parquet_file_batch_id

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


def reset_db_conn(db_stack):
    db_stack.close()
    return db_stack.enter_context(open_db_connection())


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
                    state_conn = reset_db_conn(db_stack)

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
    has_extract_failures=False,
):
    results = {}
    load_failures = []
    max_db_retries = max(1, getattr(active_settings, "db_max_retries", 3))
    tables_to_ensure = tuple(
        dict.fromkeys(
            table_name
            for task_key, table_name, _, _ in effective_task_specs
            if checkpoints[task_key].get("load_status") != "LOADED" and task_key in parquet_paths
        )
    )
    with log_step(logger, "Giai đoạn 2/2: Load streaming từ Parquet -> PostgreSQL Bronze (Atomic)"):
        with ExitStack() as db_stack:
            conn = None
            for attempt in range(1, max_db_retries + 1):
                try:
                    if is_conn_unusable(conn):
                        conn = reset_db_conn(db_stack)
                    with conn.transaction():
                        with conn.cursor() as cur:
                            ensure_schema(cur, target_schema)
                            for table_name in tables_to_ensure:
                                ensure_raw_table(cur, target_schema, table_name)
                    break
                except Exception as err:
                    if not is_transient_db_error(err) or attempt == max_db_retries:
                        raise
                    logger.warning(
                        "[DB RECONNECT %d/%d] Mất kết nối khi khởi tạo bảng Bronze: %s",
                        attempt,
                        max_db_retries,
                        err,
                    )
                    db_stack.close()
                    conn = None
                    time.sleep(2 ** (attempt - 1))

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

                if task_key not in parquet_paths:
                    continue

                parquet_path = parquet_paths[task_key]
                task_mode = task_overrides.get(task_key, {}).get("load_mode", effective_mode)
                task_wm_year = config_end_year if table_name == "raw_worldbank_indicators" else None

                rows_loaded = None
                for attempt in range(1, max_db_retries + 1):
                    try:
                        if is_conn_unusable(conn):
                            conn = reset_db_conn(db_stack)
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
                                ensure_table=False,
                                suppress_transient_failure_log=(attempt < max_db_retries),
                            )
                        break
                    except Exception as err:
                        if is_transient_db_error(err) and attempt < max_db_retries:
                            logger.warning(
                                "[DB RECONNECT %d/%d] Mất kết nối khi load %s (%s), thử kết nối lại...",
                                attempt,
                                max_db_retries,
                                task_key,
                                err,
                            )
                            db_stack.close()
                            conn = None
                            time.sleep(2 ** (attempt - 1))
                            continue

                        if table_name != "raw_worldbank_indicators" or is_transient_db_error(err):
                            raise
                        cp["load_status"] = "FAILED"
                        load_failures.append((task_key, str(err)))
                        logger.error(
                            "[ISOLATE LOAD] Giữ lại Parquet của %s và chạy tiếp các chỉ số khác: %s",
                            task_key,
                            err,
                        )
                        rows_loaded = None
                        break

                if rows_loaded is None:
                    continue

                cp["load_status"] = "LOADED"
                if not active_settings.save_raw_parquet:
                    try:
                        parquet_path.unlink(missing_ok=True)
                        logger.info("[CLEANUP] Đã xóa file Parquet sau khi commit: %s", parquet_path.name)
                    except OSError as err:
                        logger.warning("Không thể xóa file Parquet '%s': %s", parquet_path, err)
                results[table_name] = results.get(table_name, 0) + rows_loaded

            if not has_extract_failures and not load_failures:
                if is_conn_unusable(conn):
                    conn = reset_db_conn(db_stack)
                finish_run(
                    conn,
                    effective_state_schema,
                    ingestion_run_id=ingestion_run_id,
                    status="SUCCESS",
                )
    return results, load_failures


TABLE_LOAD_PRIORITY = {
    "raw_worldbank_countries": 0,
    "raw_worldbank_indicator_metadata": 1,
    "raw_worldbank_indicators": 2,
}


def build_failure_summary(failures):
    preview = "; ".join(f"{task}: {msg}" for task, msg in failures[:5])
    return f"Hoàn tất có lỗi tại {len(failures)} chỉ số ({preview})"


def run_load_only(
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
    _, config_end_year = parse_date_range(active_settings.world_bank_date_range)
    indicator_filter = set(indicators) if indicators is not None else None

    parquet_paths = {}
    missing_failures = []

    with log_step(logger, f"Chế độ LOAD-ONLY (không gọi API) | state_schema='{effective_state_schema}'"):
        with open_db_connection() as conn:
            ensure_state_schema(conn, effective_state_schema)
            resumable_run_id = (
                get_resumable_run_id(conn, effective_state_schema, run_id=effective_run_id)
                if (effective_auto_resume or effective_run_id)
                else None
            )
            if resumable_run_id:
                ingestion_run_id, resumed, checkpoints = create_or_resume_run(
                    conn,
                    effective_state_schema,
                    table_specs=(),
                    load_mode=effective_mode,
                    target_schema=target_schema,
                    data_dir=base_data_dir,
                    chunk_size=effective_chunk_size,
                    auto_resume=effective_auto_resume,
                    run_id=effective_run_id,
                )
                ordered_cps = sorted(
                    checkpoints.values(),
                    key=lambda cp: (TABLE_LOAD_PRIORITY.get(cp["table_name"], 9), cp["task_key"]),
                )
                specs = []
                for cp in ordered_cps:
                    if (
                        indicator_filter is not None
                        and cp["table_name"] == "raw_worldbank_indicators"
                        and cp.get("indicator_code") not in indicator_filter
                    ):
                        continue
                    tkey = cp["task_key"]
                    tbl = cp["table_name"]
                    specs.append((tkey, tbl, cp.get("indicator_code"), cp.get("source_endpoint") or ""))
                    if cp.get("load_status") == "LOADED":
                        continue
                    candidate = (
                        Path(cp["parquet_path"])
                        if cp.get("parquet_path")
                        else (Path(cp.get("run_data_dir") or base_data_dir) / f"{tkey}.parquet")
                    )
                    if candidate.exists():
                        parquet_paths[tkey] = candidate
                    elif tbl != "raw_worldbank_indicators":
                        raise FileNotFoundError(f"Không tìm thấy file Parquet cho bảng nền {tkey}: {candidate}")
                    else:
                        msg = f"Thiếu file Parquet cho {tkey}: {candidate}"
                        missing_failures.append((tkey, msg))
                        now = datetime.now(timezone.utc)
                        record_load_failure(
                            conn,
                            effective_state_schema,
                            ingestion_run_id=ingestion_run_id,
                            table_name=tbl,
                            batch_id=cp["batch_id"],
                            parquet_path=candidate,
                            started_at=now,
                            finished_at=now,
                            duration_seconds=0.0,
                            error_message=msg,
                            task_key=tkey,
                        )
                effective_task_specs = tuple(specs)
            else:
                discovered = discover_parquet_files(base_data_dir)
                filtered = []
                for path in discovered:
                    parts = path.stem.split("__", 1)
                    tbl = parts[0]
                    code = parts[1] if len(parts) > 1 else None
                    if indicator_filter is not None and tbl == "raw_worldbank_indicators" and code not in indicator_filter:
                        continue
                    filtered.append((path, path.stem, tbl, code))
                if not filtered:
                    raise FileNotFoundError(f"Không tìm thấy file Parquet phù hợp trong {base_data_dir}")
                filtered.sort(key=lambda item: (TABLE_LOAD_PRIORITY.get(item[2], 9), item[1]))
                effective_task_specs = tuple(
                    (tkey, tbl, code, f"file://{path.name}") for path, tkey, tbl, code in filtered
                )
                ingestion_run_id, resumed, checkpoints = create_or_resume_run(
                    conn,
                    effective_state_schema,
                    table_specs=effective_task_specs,
                    load_mode=effective_mode,
                    target_schema=target_schema,
                    data_dir=base_data_dir,
                    chunk_size=effective_chunk_size,
                    auto_resume=False,
                    run_id=None,
                )
                cp_updates = []
                for path, tkey, _, _ in filtered:
                    cp = checkpoints[tkey]
                    embedded_batch = read_parquet_file_batch_id(path)
                    if embedded_batch:
                        cp["batch_id"] = embedded_batch
                    cp["parquet_path"] = str(path)
                    cp["extract_status"] = "EXTRACTED"
                    parquet_paths[tkey] = path
                    cp_updates.append((cp["batch_id"], str(path), ingestion_run_id, tkey))
                if cp_updates:
                    cp_ref = table_ref(effective_state_schema, CHECKPOINTS_TABLE)
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.executemany(
                                sql.SQL(
                                    "UPDATE {} SET batch_id = %s, parquet_path = %s, extract_status = 'EXTRACTED', updated_at = CURRENT_TIMESTAMP WHERE ingestion_run_id = %s AND task_key = %s"
                                ).format(cp_ref),
                                cp_updates,
                            )

    try:
        with log_step(logger, f"ELT Load-Only Pipeline (ingestion_run_id={ingestion_run_id}, files={len(parquet_paths)})"):
            results, load_failures = run_load_stage(
                effective_task_specs,
                checkpoints,
                parquet_paths,
                {},
                target_schema=target_schema,
                effective_state_schema=effective_state_schema,
                effective_mode=effective_mode,
                effective_chunk_size=effective_chunk_size,
                config_end_year=config_end_year,
                current_api_updated="",
                ingestion_run_id=ingestion_run_id,
                active_settings=active_settings,
                has_extract_failures=bool(missing_failures),
            )
            all_failures = [*missing_failures, *load_failures]
            if all_failures:
                raise RuntimeError(build_failure_summary(all_failures))
            return results
    except Exception as err:
        mark_run_failed(effective_state_schema, ingestion_run_id, str(err))
        raise


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
