from psycopg import sql

from src.reliability.schema import CHECKPOINTS_TABLE, STEP_LOGS_TABLE
from src.storage.database import table_ref


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
