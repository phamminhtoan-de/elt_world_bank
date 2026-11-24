from datetime import datetime, timezone
from pathlib import Path
import uuid
from psycopg import sql

from src.reliability.schema import (
    CHECKPOINT_COLUMNS,
    CHECKPOINTS_TABLE,
    RUNS_TABLE,
    normalize_task_spec,
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

            if normalized_specs:
                cur.executemany(
                    sql.SQL(
                        "INSERT INTO {} (ingestion_run_id, task_key, table_name, indicator_code, batch_id, source_endpoint) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (ingestion_run_id, task_key) DO NOTHING"
                    ).format(cp_ref),
                    [
                        (resolved_run_id, task_key, table_name, indicator_code, str(uuid.uuid4()), source_endpoint)
                        for task_key, table_name, indicator_code, source_endpoint in normalized_specs
                    ],
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
