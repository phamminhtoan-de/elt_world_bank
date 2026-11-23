from psycopg import sql

from src.storage.database import table_ref

RUNS_TABLE = "ingestion_runs"
CHECKPOINTS_TABLE = "ingestion_checkpoints"
STEP_LOGS_TABLE = "ingestion_step_logs"
WATERMARKS_TABLE = "ingestion_watermarks"

CHECKPOINT_COLUMNS = (
    "task_key",
    "table_name",
    "indicator_code",
    "batch_id",
    "source_endpoint",
    "parquet_path",
    "extract_status",
    "load_status",
    "rows_extracted",
    "rows_loaded",
    "last_extracted_page",
    "last_checkpoint_key",
    "extracted_at",
    "loaded_at",
)


def normalize_task_spec(spec):
    if len(spec) == 2:
        table_name, source_endpoint = spec
        return table_name, table_name, None, source_endpoint
    if len(spec) == 4:
        return spec
    raise ValueError(f"task spec không hợp lệ: {spec}")


def ensure_state_schema(conn, state_schema):
    runs_ref = table_ref(state_schema, RUNS_TABLE)
    cp_ref = table_ref(state_schema, CHECKPOINTS_TABLE)
    logs_ref = table_ref(state_schema, STEP_LOGS_TABLE)
    wm_ref = table_ref(state_schema, WATERMARKS_TABLE)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(state_schema)))
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        ingestion_run_id VARCHAR(36) PRIMARY KEY,
                        status           VARCHAR(20) NOT NULL,
                        load_mode        VARCHAR(20) NOT NULL,
                        target_schema    VARCHAR(100) NOT NULL,
                        data_dir         TEXT NOT NULL,
                        chunk_size       INTEGER NOT NULL,
                        started_at       TIMESTAMPTZ NOT NULL,
                        finished_at      TIMESTAMPTZ,
                        error_message    TEXT
                    )
                    """
                ).format(runs_ref)
            )
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        id                  BIGSERIAL PRIMARY KEY,
                        ingestion_run_id    VARCHAR(36) NOT NULL REFERENCES {}(ingestion_run_id),
                        task_key            VARCHAR(150) NOT NULL,
                        table_name          VARCHAR(100) NOT NULL,
                        indicator_code      VARCHAR(50),
                        batch_id            VARCHAR(64) NOT NULL,
                        source_endpoint     TEXT NOT NULL,
                        parquet_path        TEXT,
                        extract_status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                        load_status         VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                        rows_extracted      INTEGER NOT NULL DEFAULT 0,
                        rows_loaded         INTEGER NOT NULL DEFAULT 0,
                        last_extracted_page INTEGER NOT NULL DEFAULT 0,
                        last_checkpoint_key TEXT,
                        extracted_at        TIMESTAMPTZ,
                        loaded_at           TIMESTAMPTZ,
                        updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        error_message       TEXT,
                        UNIQUE (ingestion_run_id, task_key)
                    )
                    """
                ).format(cp_ref, runs_ref)
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS task_key VARCHAR(150), ADD COLUMN IF NOT EXISTS indicator_code VARCHAR(50)"
                ).format(cp_ref)
            )
            cur.execute(sql.SQL("UPDATE {} SET task_key = table_name WHERE task_key IS NULL").format(cp_ref))
            cur.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN task_key SET NOT NULL").format(cp_ref))
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {} DROP CONSTRAINT IF EXISTS ingestion_checkpoints_ingestion_run_id_table_name_key"
                ).format(cp_ref)
            )
            cur.execute(
                sql.SQL(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_ingestion_checkpoints_run_task ON {} (ingestion_run_id, task_key)"
                ).format(cp_ref)
            )
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        id               BIGSERIAL PRIMARY KEY,
                        ingestion_run_id VARCHAR(36) NOT NULL,
                        batch_id         VARCHAR(64) NOT NULL,
                        task_key         VARCHAR(150),
                        table_name       VARCHAR(100) NOT NULL,
                        stage            VARCHAR(20) NOT NULL,
                        status           VARCHAR(20) NOT NULL,
                        row_count        INTEGER NOT NULL DEFAULT 0,
                        parquet_path     TEXT,
                        started_at       TIMESTAMPTZ NOT NULL,
                        finished_at      TIMESTAMPTZ NOT NULL,
                        duration_seconds DOUBLE PRECISION NOT NULL,
                        error_message    TEXT
                    )
                    """
                ).format(logs_ref)
            )
            cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS task_key VARCHAR(150)").format(logs_ref))
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        target_schema    VARCHAR(100) NOT NULL,
                        task_key         VARCHAR(150) NOT NULL,
                        table_name       VARCHAR(100) NOT NULL,
                        indicator_code   VARCHAR(50),
                        last_loaded_year INTEGER,
                        api_last_updated VARCHAR(30),
                        last_run_id      VARCHAR(36),
                        updated_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (target_schema, task_key)
                    )
                    """
                ).format(wm_ref)
            )
