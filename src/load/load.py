from datetime import datetime, timezone
from pathlib import Path
import time
import pandas as pd
from psycopg import sql
import pyarrow.parquet as pq

from src.config.logger import get_logger, log_step
from src.config.settings import VALID_LOAD_MODES, settings
from src.load.serializer import (
    INT_SERIALIZE_COLUMNS,
    attach_missing_raw_payload,
    serialize_dataframe_chunk,
    serialize_records_chunk,
)
from src.load.sql_ops import (
    batch_already_loaded_in_cur,
    prepare_copy_target,
    upsert_from_temp_table,
    validate_loaded_batch_in_cur,
)
from src.metadata.state import (
    record_load_failure,
    update_load_checkpoint_in_cur,
    upsert_watermark_in_cur,
)
from src.storage.database import (
    TABLE_DDL_COLUMNS,
    ensure_raw_table,
    ensure_schema,
    open_db_connection,
)
from src.storage.parquet_io import iter_parquet_dataframe_chunks, read_parquet_batch_id

logger = get_logger(__name__)


def extract_chunk_metrics(chunk):
    max_year = None
    max_lu = ""
    if not isinstance(chunk, pd.DataFrame):
        return max_year, max_lu
    if "year" in chunk.columns:
        year_vals = pd.to_numeric(chunk["year"], errors="coerce").dropna()
        if not year_vals.empty:
            max_year = int(year_vals.max())
    if "last_updated" in chunk.columns:
        lu_vals = chunk["last_updated"].dropna().astype(str).str.strip()
        lu_vals = lu_vals[lu_vals != ""]
        if not lu_vals.empty:
            max_lu = str(lu_vals.max())
    return max_year, max_lu


def stream_chunks_to_postgres(
    conn,
    *,
    table_name,
    columns,
    chunks_iter,
    batch_id="",
    schema,
    load_mode,
    state_schema=None,
    ingestion_run_id=None,
    parquet_path=None,
    task_key=None,
    indicator_code=None,
    expected_rows=None,
    watermark_year=None,
    api_last_updated=None,
    auto_populate_raw_payload=None,
    skip_duplicate_batch=None,
    ensure_table=True,
):
    if load_mode not in VALID_LOAD_MODES:
        raise ValueError(f"load_mode không hợp lệ: '{load_mode}'. Chỉ chấp nhận {sorted(VALID_LOAD_MODES)}.")

    full_table = f"{schema}.{table_name}"
    resolved_task_key = task_key or table_name
    started_at = datetime.now(timezone.utc)
    start_perf = time.perf_counter()
    rows_loaded = 0
    chunk_count = 0
    observed_max_year = None
    observed_last_updated = ""

    should_auto_populate = (
        settings.backfill_parquet_auto_populate_raw_payload
        if auto_populate_raw_payload is None
        else auto_populate_raw_payload
    )
    should_skip_duplicate = (
        settings.elt_skip_duplicate_batch if skip_duplicate_batch is None else skip_duplicate_batch
    )
    has_batch_col = "batch_id" in columns
    has_raw_payload = "raw_payload" in columns
    need_raw_payload = (not has_raw_payload) and should_auto_populate
    copy_columns = tuple(columns)
    if need_raw_payload:
        copy_columns = (*copy_columns, "raw_payload")
    if not has_batch_col:
        copy_columns = (*copy_columns, "batch_id")
    serialize_columns = (
        copy_columns if has_batch_col else tuple(col for col in copy_columns if col != "batch_id")
    )
    extra_batch_id = None if has_batch_col else batch_id

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                if ensure_table:
                    ensure_raw_table(cur, schema, table_name)
                if (
                    load_mode == "append"
                    and should_skip_duplicate
                    and batch_already_loaded_in_cur(
                        cur, schema, table_name, batch_id, indicator_code=indicator_code
                    )
                ):
                    duration = time.perf_counter() - start_perf
                    finished_at = datetime.now(timezone.utc)
                    if state_schema and ingestion_run_id:
                        update_load_checkpoint_in_cur(
                            cur,
                            state_schema,
                            ingestion_run_id=ingestion_run_id,
                            table_name=table_name,
                            batch_id=batch_id,
                            parquet_path=parquet_path,
                            rows_loaded=0,
                            loaded_at=finished_at,
                            started_at=started_at,
                            finished_at=finished_at,
                            duration_seconds=duration,
                            status="LOADED",
                            task_key=resolved_task_key,
                        )
                    logger.info(
                        "  -> [SKIP APPEND] %s đã tồn tại batch_id=%s; bỏ qua để tránh nạp trùng.",
                        full_table,
                        batch_id,
                    )
                    return 0

                copy_target, temp_table = prepare_copy_target(cur, schema, table_name, load_mode, indicator_code)
                copy_query = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '')").format(
                    copy_target, sql.SQL(", ").join(sql.Identifier(col) for col in copy_columns)
                )
                with cur.copy(copy_query) as copy:
                    for chunk in chunks_iter:
                        if len(chunk) == 0:
                            continue
                        chunk_for_copy = (
                            attach_missing_raw_payload(chunk, columns) if need_raw_payload else chunk
                        )
                        chunk_max_year, chunk_lu = extract_chunk_metrics(chunk_for_copy)
                        if chunk_max_year is not None and (
                            observed_max_year is None or chunk_max_year > observed_max_year
                        ):
                            observed_max_year = chunk_max_year
                        if chunk_lu > observed_last_updated:
                            observed_last_updated = chunk_lu
                        copy.write(
                            serialize_dataframe_chunk(chunk_for_copy, serialize_columns, extra_batch_id)
                        )
                        rows_loaded += len(chunk_for_copy)
                        chunk_count += 1
                        logger.info(
                            "  -> %s | Đã stream chunk %d (+%d dòng, tổng %s dòng)",
                            full_table,
                            chunk_count,
                            len(chunk),
                            f"{rows_loaded:,}",
                        )

                if load_mode == "upsert" and rows_loaded > 0:
                    upsert_from_temp_table(cur, schema, table_name, temp_table, copy_columns)

                validate_loaded_batch_in_cur(
                    cur, table_name=table_name, expected_rows=expected_rows, rows_loaded=rows_loaded
                )

                duration = time.perf_counter() - start_perf
                finished_at = datetime.now(timezone.utc)
                if state_schema and ingestion_run_id:
                    update_load_checkpoint_in_cur(
                        cur,
                        state_schema,
                        ingestion_run_id=ingestion_run_id,
                        table_name=table_name,
                        batch_id=batch_id,
                        parquet_path=parquet_path,
                        rows_loaded=rows_loaded,
                        loaded_at=finished_at,
                        started_at=started_at,
                        finished_at=finished_at,
                        duration_seconds=duration,
                        status="LOADED",
                        task_key=resolved_task_key,
                    )
                    if rows_loaded > 0:
                        upsert_watermark_in_cur(
                            cur,
                            state_schema,
                            target_schema=schema,
                            task_key=resolved_task_key,
                            table_name=table_name,
                            indicator_code=indicator_code,
                            last_loaded_year=observed_max_year,
                            api_last_updated=(observed_last_updated or api_last_updated or None),
                            last_run_id=ingestion_run_id,
                        )

        logger.info("  -> Đã nạp %s dòng vào %s (mode=%s, batch_id=%s).", f"{rows_loaded:,}", full_table, load_mode, batch_id)
        return rows_loaded

    except Exception as err:
        duration = time.perf_counter() - start_perf
        finished_at = datetime.now(timezone.utc)
        if state_schema and ingestion_run_id:
            try:
                record_load_failure(
                    conn,
                    state_schema,
                    ingestion_run_id=ingestion_run_id,
                    table_name=table_name,
                    batch_id=batch_id,
                    parquet_path=parquet_path,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=duration,
                    error_message=str(err),
                    task_key=resolved_task_key,
                )
            except Exception as state_err:
                logger.warning("Không thể ghi lỗi vào bảng checkpoint %s.ingestion_checkpoints: %s", state_schema, state_err)
        raise


def load_parquet_to_postgres(
    conn,
    parquet_path,
    *,
    batch_id,
    schema,
    load_mode,
    table_name=None,
    chunk_size=None,
    state_schema=None,
    ingestion_run_id=None,
    task_key=None,
    indicator_code=None,
    watermark_year=None,
    api_last_updated=None,
    auto_populate_raw_payload=None,
    skip_duplicate_batch=None,
    ensure_table=True,
):
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file Parquet: {path}")

    parquet_file = pq.ParquetFile(path)
    columns = tuple(parquet_file.schema_arrow.names)
    expected_rows = parquet_file.metadata.num_rows
    effective_batch_id = read_parquet_batch_id(parquet_file) or batch_id
    del parquet_file

    parts = path.stem.split("__", 1)
    resolved_table = table_name or parts[0]
    resolved_indicator = (
        indicator_code if indicator_code is not None else (parts[1] if len(parts) > 1 else None)
    )
    effective_chunk_size = chunk_size if chunk_size is not None else settings.elt_chunk_size

    return stream_chunks_to_postgres(
        conn,
        table_name=resolved_table,
        columns=columns,
        chunks_iter=iter_parquet_dataframe_chunks(path, chunk_size=effective_chunk_size),
        batch_id=effective_batch_id,
        schema=schema,
        load_mode=load_mode,
        state_schema=state_schema,
        ingestion_run_id=ingestion_run_id,
        parquet_path=path,
        task_key=task_key,
        indicator_code=resolved_indicator,
        expected_rows=expected_rows,
        watermark_year=watermark_year,
        api_last_updated=api_last_updated,
        auto_populate_raw_payload=auto_populate_raw_payload,
        skip_duplicate_batch=skip_duplicate_batch,
        ensure_table=ensure_table,
    )


def discover_parquet_files(data_dir=None):
    target_dir = Path(data_dir) if data_dir is not None else settings.raw_data_dir
    parquet_paths = sorted(
        path
        for path in target_dir.glob("*.parquet")
        if path.stem.split("__")[0] in TABLE_DDL_COLUMNS
    )
    if not parquet_paths:
        raise FileNotFoundError(f"Không tìm thấy file Parquet nào trong {target_dir}")
    return parquet_paths


def load_parquet_files(
    parquet_paths=None,
    *,
    batch_id,
    schema=None,
    load_mode=None,
    chunk_size=None,
    state_schema=None,
    ingestion_run_id=None,
):
    target_schema = schema or settings.tgt_db_schema
    effective_mode = (load_mode or settings.elt_load_mode).lower()
    if effective_mode not in VALID_LOAD_MODES:
        raise ValueError(f"load_mode không hợp lệ: '{effective_mode}'. Chỉ chấp nhận {sorted(VALID_LOAD_MODES)}.")

    paths = [Path(p) for p in parquet_paths] if parquet_paths is not None else discover_parquet_files()
    unique_tables = tuple(dict.fromkeys(path.stem.split("__", 1)[0] for path in paths))
    loaded_counts = {}

    with log_step(logger, f"Kết nối PostgreSQL ({settings.db_dsn_safe})"):
        with open_db_connection() as conn:
            with log_step(logger, f"Đảm bảo schema '{target_schema}'"):
                with conn.transaction():
                    with conn.cursor() as cur:
                        ensure_schema(cur, target_schema)
                        for table_name in unique_tables:
                            ensure_raw_table(cur, target_schema, table_name)

            for path in paths:
                parts = path.stem.split("__", 1)
                table_name = parts[0]
                indicator_code = parts[1] if len(parts) > 1 else None
                with log_step(logger, f"Stream Parquet {path.name} -> {target_schema}.{table_name}"):
                    rows_loaded = load_parquet_to_postgres(
                        conn,
                        path,
                        batch_id=batch_id,
                        schema=target_schema,
                        load_mode=effective_mode,
                        table_name=table_name,
                        chunk_size=chunk_size,
                        state_schema=state_schema,
                        ingestion_run_id=ingestion_run_id,
                        task_key=path.stem,
                        indicator_code=indicator_code,
                        ensure_table=False,
                    )
                loaded_counts[table_name] = loaded_counts.get(table_name, 0) + rows_loaded

    logger.info("Đã đóng kết nối PostgreSQL.")
    return loaded_counts
