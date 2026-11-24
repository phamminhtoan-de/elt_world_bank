from contextlib import ExitStack
import time

from src.config.logger import get_logger, log_step
from src.load.loader import load_parquet_to_postgres
from src.reliability.recovery import reset_db_conn
from src.reliability.runs import finish_run
from src.storage.database import (
    ensure_raw_table,
    ensure_schema,
    is_conn_unusable,
    is_transient_db_error,
    open_db_connection,
)

logger = get_logger("elt.pipeline")


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
                        conn = reset_db_conn(db_stack, open_conn_fn=open_db_connection)
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
                            conn = reset_db_conn(db_stack, open_conn_fn=open_db_connection)
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
                    conn = reset_db_conn(db_stack, open_conn_fn=open_db_connection)
                finish_run(
                    conn,
                    effective_state_schema,
                    ingestion_run_id=ingestion_run_id,
                    status="SUCCESS",
                )
    return results, load_failures
