from datetime import datetime, timezone
from pathlib import Path
import uuid
from psycopg import sql

from src.metadata.schema import (
    CHECKPOINT_COLUMNS,
    CHECKPOINTS_TABLE,
    RUNS_TABLE,
    STEP_LOGS_TABLE,
    WATERMARKS_TABLE,
    ensure_state_schema,
    normalize_task_spec,
)
from src.metadata.watermarks import (
    get_previously_loaded_tasks,
    get_watermarks,
    table_has_rows,
    upsert_watermark_in_cur,
)
from src.storage.database import table_ref


def create_or_resume_run(
    conn,
    state_schema,
    *,
    table_specs,
    load_mode,
    target_schema,
    data_dir,
    chunk_size,
    auto_resume=True,
    run_id=None,
):
    now = datetime.now(timezone.utc)
    normalized_specs = [normalize_task_spec(spec) for spec in table_specs]
    runs_ref = table_ref(state_schema, RUNS_TABLE)
    cp_ref = table_ref(state_schema, CHECKPOINTS_TABLE)

    with conn.transaction():
        with conn.cursor() as cur:
            resumed = False
            resolved_run_id = None
            run_data_dir = None

            if run_id:
                cur.execute(
                    sql.SQL("SELECT ingestion_run_id, data_dir FROM {} WHERE ingestion_run_id = %s").format(runs_ref),
                    (run_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"Không tìm thấy ingestion_run_id '{run_id}' trong {state_schema}.{RUNS_TABLE}.")
                resolved_run_id, raw_dir = row[0], row[1]
                run_data_dir = Path(raw_dir) if raw_dir else Path(data_dir)
                resumed = True
            elif auto_resume:
                cur.execute(
                    sql.SQL(
                        "SELECT ingestion_run_id, data_dir FROM {} WHERE status IN ('RUNNING', 'FAILED') ORDER BY started_at DESC LIMIT 1"
                    ).format(runs_ref)
                )
                row = cur.fetchone()
                if row:
                    resolved_run_id, raw_dir = row[0], row[1]
                    run_data_dir = Path(raw_dir) if raw_dir else Path(data_dir)
                    resumed = True

            if resumed:
                run_data_dir.mkdir(parents=True, exist_ok=True)
                cur.execute(
                    sql.SQL(
                        "UPDATE {} SET status = 'RUNNING', finished_at = NULL, error_message = NULL WHERE ingestion_run_id = %s"
                    ).format(runs_ref),
                    (resolved_run_id,),
                )
            else:
                resolved_run_id = str(uuid.uuid4())
                run_data_dir = Path(data_dir)
                run_data_dir.mkdir(parents=True, exist_ok=True)
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} (ingestion_run_id, status, load_mode, target_schema, data_dir, chunk_size, started_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    ).format(runs_ref),
                    (resolved_run_id, "RUNNING", load_mode, target_schema, str(run_data_dir), chunk_size, now),
                )

            for task_key, table_name, indicator_code, source_endpoint in normalized_specs:
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} (ingestion_run_id, task_key, table_name, indicator_code, batch_id, source_endpoint) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (ingestion_run_id, task_key) DO NOTHING"
                    ).format(cp_ref),
                    (resolved_run_id, task_key, table_name, indicator_code, str(uuid.uuid4()), source_endpoint),
                )

            select_cols = sql.SQL(", ").join(sql.Identifier(col) for col in CHECKPOINT_COLUMNS)
            cur.execute(
                sql.SQL("SELECT {} FROM {} WHERE ingestion_run_id = %s").format(select_cols, cp_ref),
                (resolved_run_id,),
            )
            checkpoints = {}
            for row in cur.fetchall():
                cp = dict(zip(CHECKPOINT_COLUMNS, row))
                cp["run_data_dir"] = str(run_data_dir)
                checkpoints[cp["task_key"]] = cp

            return resolved_run_id, resumed, checkpoints


def insert_step_log_in_cur(
    cur,
    state_schema,
    *,
    ingestion_run_id,
    batch_id,
    task_key,
    table_name,
    stage,
    status,
    row_count,
    parquet_path,
    started_at,
    finished_at,
    duration_seconds,
    error_message=None,
):
    logs_ref = table_ref(state_schema, STEP_LOGS_TABLE)
    cur.execute(
        sql.SQL(
            """
            INSERT INTO {} (
                ingestion_run_id, batch_id, task_key, table_name, stage, status,
                row_count, parquet_path, started_at, finished_at, duration_seconds, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        ).format(logs_ref),
        (
            ingestion_run_id,
            batch_id,
            task_key,
            table_name,
            stage,
            status,
            row_count,
            str(parquet_path) if parquet_path is not None else None,
            started_at,
            finished_at,
            round(duration_seconds, 4),
            error_message,
        ),
    )


def save_extract_checkpoint(
    conn,
    state_schema,
    *,
    ingestion_run_id,
    table_name,
    batch_id,
    source_endpoint,
    parquet_path,
    rows_extracted,
    last_extracted_page,
    last_checkpoint_key,
    extracted_at,
    started_at,
    finished_at,
    duration_seconds,
    status="EXTRACTED",
    error_message=None,
    task_key=None,
):
    resolved_task_key = task_key or table_name
    step_status = (
        "SUCCESS"
        if status == "EXTRACTED"
        else ("FILE_READY" if status == "FILE_READY" else "FAILED")
    )
    cp_ref = table_ref(state_schema, CHECKPOINTS_TABLE)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    UPDATE {}
                    SET source_endpoint = %s,
                        parquet_path = %s,
                        extract_status = %s,
                        rows_extracted = %s,
                        last_extracted_page = %s,
                        last_checkpoint_key = %s,
                        extracted_at = %s,
                        updated_at = CURRENT_TIMESTAMP,
                        error_message = %s
                    WHERE ingestion_run_id = %s AND task_key = %s
                    """
                ).format(cp_ref),
                (
                    source_endpoint,
                    str(parquet_path) if parquet_path is not None else None,
                    status,
                    rows_extracted,
                    last_extracted_page,
                    last_checkpoint_key,
                    extracted_at,
                    error_message,
                    ingestion_run_id,
                    resolved_task_key,
                ),
            )
            insert_step_log_in_cur(
                cur,
                state_schema,
                ingestion_run_id=ingestion_run_id,
                batch_id=batch_id,
                task_key=resolved_task_key,
                table_name=table_name,
                stage="EXTRACT",
                status=step_status,
                row_count=rows_extracted,
                parquet_path=parquet_path,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration_seconds,
                error_message=error_message,
            )


def update_load_checkpoint_in_cur(
    cur,
    state_schema,
    *,
    ingestion_run_id,
    table_name,
    batch_id,
    parquet_path,
    rows_loaded,
    loaded_at,
    started_at,
    finished_at,
    duration_seconds,
    status="LOADED",
    error_message=None,
    task_key=None,
):
    resolved_task_key = task_key or table_name
    step_status = "SUCCESS" if status == "LOADED" else "FAILED"
    cp_ref = table_ref(state_schema, CHECKPOINTS_TABLE)

    cur.execute(
        sql.SQL(
            """
            UPDATE {}
            SET load_status = %s,
                rows_loaded = %s,
                loaded_at = %s,
                updated_at = CURRENT_TIMESTAMP,
                error_message = %s
            WHERE ingestion_run_id = %s AND task_key = %s
            """
        ).format(cp_ref),
        (status, rows_loaded, loaded_at, error_message, ingestion_run_id, resolved_task_key),
    )
    insert_step_log_in_cur(
        cur,
        state_schema,
        ingestion_run_id=ingestion_run_id,
        batch_id=batch_id,
        task_key=resolved_task_key,
        table_name=table_name,
        stage="LOAD",
        status=step_status,
        row_count=rows_loaded,
        parquet_path=parquet_path,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        error_message=error_message,
    )


def record_load_failure(
    conn,
    state_schema,
    *,
    ingestion_run_id,
    table_name,
    batch_id,
    parquet_path,
    started_at,
    finished_at,
    duration_seconds,
    error_message,
    task_key=None,
):
    with conn.transaction():
        with conn.cursor() as cur:
            update_load_checkpoint_in_cur(
                cur,
                state_schema,
                ingestion_run_id=ingestion_run_id,
                table_name=table_name,
                batch_id=batch_id,
                parquet_path=parquet_path,
                rows_loaded=0,
                loaded_at=None,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration_seconds,
                status="FAILED",
                error_message=error_message,
                task_key=task_key,
            )


def finish_run(conn, state_schema, *, ingestion_run_id, status, error_message=None):
    finished_at = datetime.now(timezone.utc)
    runs_ref = table_ref(state_schema, RUNS_TABLE)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("UPDATE {} SET status = %s, finished_at = %s, error_message = %s WHERE ingestion_run_id = %s").format(
                    runs_ref
                ),
                (status, finished_at, error_message, ingestion_run_id),
            )


def get_resumable_run_id(conn, state_schema, run_id=None):
    if run_id:
        return run_id
    runs_ref = table_ref(state_schema, RUNS_TABLE)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT ingestion_run_id FROM {} WHERE status IN ('RUNNING', 'FAILED') ORDER BY started_at DESC LIMIT 1"
                ).format(runs_ref)
            )
            row = cur.fetchone()
            return row[0] if row else None
