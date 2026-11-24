from psycopg import sql

from src.metadata.schema import CHECKPOINTS_TABLE, RUNS_TABLE, WATERMARKS_TABLE
from src.storage.database import table_ref


def table_has_rows(cur, schema, table_name):
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
        (schema, table_name),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return False
    cur.execute(sql.SQL("SELECT 1 FROM {} LIMIT 1").format(table_ref(schema, table_name)))
    return bool(cur.fetchone())


def get_previously_loaded_tasks(conn, state_schema, target_schema, exclude_run_id=None):
    runs_ref = table_ref(state_schema, RUNS_TABLE)
    cp_ref = table_ref(state_schema, CHECKPOINTS_TABLE)
    params = [target_schema]
    exclude_clause = sql.SQL("")
    if exclude_run_id:
        exclude_clause = sql.SQL(" AND cp.ingestion_run_id <> %s")
        params.append(exclude_run_id)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT DISTINCT cp.task_key, cp.table_name FROM {} AS cp JOIN {} AS r ON r.ingestion_run_id = cp.ingestion_run_id WHERE r.target_schema = %s AND cp.load_status = 'LOADED'{}"
                ).format(cp_ref, runs_ref, exclude_clause),
                params,
            )
            rows = cur.fetchall()
            table_ready = {}
            loaded_tasks = set()
            for task_key, table_name in rows:
                if table_name not in table_ready:
                    table_ready[table_name] = table_has_rows(cur, target_schema, table_name)
                if table_ready[table_name]:
                    loaded_tasks.add(task_key)
            return loaded_tasks


def upsert_watermark_in_cur(
    cur,
    state_schema,
    *,
    target_schema,
    task_key,
    table_name,
    indicator_code=None,
    last_loaded_year=None,
    api_last_updated=None,
    last_run_id=None,
):
    wm_ref = table_ref(state_schema, WATERMARKS_TABLE)
    clean_updated = (api_last_updated or "").strip() or None
    cur.execute(
        sql.SQL(
            """
            INSERT INTO {wm} (
                target_schema, task_key, table_name, indicator_code,
                last_loaded_year, api_last_updated, last_run_id, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (target_schema, task_key) DO UPDATE SET
                table_name = EXCLUDED.table_name,
                indicator_code = COALESCE(EXCLUDED.indicator_code, {wm}.indicator_code),
                last_loaded_year = GREATEST({wm}.last_loaded_year, EXCLUDED.last_loaded_year),
                api_last_updated = GREATEST({wm}.api_last_updated, EXCLUDED.api_last_updated),
                last_run_id = COALESCE(EXCLUDED.last_run_id, {wm}.last_run_id),
                updated_at = CURRENT_TIMESTAMP
            """
        ).format(wm=wm_ref),
        (
            target_schema,
            task_key,
            table_name,
            indicator_code,
            last_loaded_year,
            clean_updated,
            last_run_id,
        ),
    )


def get_watermarks(conn, state_schema, target_schema, exclude_run_id=None):
    wm_ref = table_ref(state_schema, WATERMARKS_TABLE)
    params = [target_schema]
    exclude_clause = sql.SQL("")
    if exclude_run_id:
        exclude_clause = sql.SQL(" AND (last_run_id IS NULL OR last_run_id <> %s)")
        params.append(exclude_run_id)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT task_key, table_name, indicator_code, last_loaded_year, api_last_updated, last_run_id FROM {} WHERE target_schema = %s{}"
                ).format(wm_ref, exclude_clause),
                params,
            )
            rows = cur.fetchall()
            table_ready = {}
            watermarks = {}
            for task_key, table_name, indicator_code, last_loaded_year, api_last_updated, last_run_id in rows:
                if table_name not in table_ready:
                    table_ready[table_name] = table_has_rows(cur, target_schema, table_name)
                if table_ready[table_name]:
                    watermarks[task_key] = {
                        "table_name": table_name,
                        "indicator_code": indicator_code,
                        "last_loaded_year": last_loaded_year,
                        "api_last_updated": api_last_updated or "",
                        "last_run_id": last_run_id,
                    }
            return watermarks
