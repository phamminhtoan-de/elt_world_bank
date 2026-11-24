from contextlib import contextmanager
import psycopg
from psycopg import sql

from src.config.settings import settings

TABLE_DDL_COLUMNS = {
    "raw_worldbank_indicators": (
        ("country_iso3", "VARCHAR(10)"),
        ("country_iso2", "VARCHAR(10)"),
        ("country_name", "TEXT"),
        ("indicator_code", "VARCHAR(50)"),
        ("indicator_name", "TEXT"),
        ("year", "INTEGER"),
        ("value", "DOUBLE PRECISION"),
        ("unit", "TEXT"),
        ("obs_status", "VARCHAR(50)"),
        ("decimal", "INTEGER"),
        ("source_id", "VARCHAR(20)"),
        ("last_updated", "DATE"),
    ),
    "raw_worldbank_countries": (
        ("country_iso3", "VARCHAR(10)"),
        ("country_iso2", "VARCHAR(10)"),
        ("country_name", "TEXT"),
        ("region_id", "VARCHAR(20)"),
        ("region_iso2", "VARCHAR(20)"),
        ("region_name", "TEXT"),
        ("admin_region_id", "VARCHAR(20)"),
        ("admin_region_name", "TEXT"),
        ("income_level_id", "VARCHAR(20)"),
        ("income_level_name", "TEXT"),
        ("lending_type_id", "VARCHAR(20)"),
        ("lending_type_name", "TEXT"),
        ("capital_city", "TEXT"),
        ("longitude", "TEXT"),
        ("latitude", "TEXT"),
    ),
    "raw_worldbank_indicator_metadata": (
        ("indicator_code", "VARCHAR(50)"),
        ("indicator_name", "TEXT"),
        ("unit", "TEXT"),
        ("source_id", "VARCHAR(20)"),
        ("source_name", "TEXT"),
        ("source_note", "TEXT"),
        ("source_organization", "TEXT"),
        ("topics", "TEXT"),
    ),
}

TABLE_PRIMARY_KEYS = {
    "raw_worldbank_indicators": ("country_iso2", "indicator_code", "year"),
    "raw_worldbank_countries": ("country_iso3",),
    "raw_worldbank_indicator_metadata": ("indicator_code",),
}

INGESTION_META_DDL_COLUMNS = (
    ("raw_payload", "JSONB"),
    ("extracted_at", "TIMESTAMPTZ"),
    ("batch_id", "VARCHAR(64)"),
    ("source_endpoint", "TEXT"),
    ("ingestion_run_id", "VARCHAR(36)"),
    ("ingested_at", "TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"),
)

TABLE_INDEXES = {
    "raw_worldbank_indicators": (
        ("idx_raw_worldbank_indicators_batch_id_indicator_code", ("batch_id", "indicator_code")),
    ),
    "raw_worldbank_countries": (
        ("idx_raw_worldbank_countries_batch_id", ("batch_id",)),
    ),
    "raw_worldbank_indicator_metadata": (
        ("idx_raw_worldbank_indicator_metadata_batch_id", ("batch_id",)),
    ),
}

OBSOLETE_INDEXES = {
    "raw_worldbank_indicators": (
        "idx_raw_worldbank_indicators_batch_id",
        "idx_raw_worldbank_indicators_indicator_code",
    ),
}


def table_ref(schema, table_name):
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table_name))


def get_connection():
    return psycopg.connect(
        host=settings.tgt_db_host,
        port=settings.tgt_db_port,
        dbname=settings.tgt_db_name,
        user=settings.tgt_db_user,
        password=settings.tgt_db_password,
    )


@contextmanager
def open_db_connection():
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def ensure_schema(cur, schema):
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))


def ensure_raw_table(cur, schema, table_name):
    if table_name not in TABLE_DDL_COLUMNS:
        raise ValueError(f"Bảng '{table_name}' chưa được khai báo DDL trong TABLE_DDL_COLUMNS.")

    target_ref = table_ref(schema, table_name)
    all_cols = (*TABLE_DDL_COLUMNS[table_name], *INGESTION_META_DDL_COLUMNS)
    column_defs = [sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t)) for c, t in all_cols]
    cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(target_ref, sql.SQL(", ").join(column_defs)))

    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
        (schema, table_name),
    )
    existing_cols = {row[0] for row in cur.fetchall()}
    missing_meta_cols = [
        (c, t) for c, t in INGESTION_META_DDL_COLUMNS if c not in existing_cols
    ]
    if missing_meta_cols:
        alter_clauses = [
            sql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(sql.Identifier(c), sql.SQL(t))
            for c, t in missing_meta_cols
        ]
        cur.execute(sql.SQL("ALTER TABLE {} {}").format(target_ref, sql.SQL(", ").join(alter_clauses)))

    for obsolete_idx in OBSOLETE_INDEXES.get(table_name, ()):
        cur.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(table_ref(schema, obsolete_idx)))

    for idx_name, idx_cols in TABLE_INDEXES.get(table_name, ()):
        cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in idx_cols)
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(idx_name), target_ref, cols_sql
            )
        )


BACKFILL_EXCLUDED_META_COLUMNS = tuple(col for col, _ in INGESTION_META_DDL_COLUMNS)


def backfill_missing_raw_payloads(
    conn,
    schema,
    *,
    tables=None,
    exclude_meta_columns=None,
    only_null=True,
):
    target_tables = tuple(tables) if tables is not None else tuple(TABLE_DDL_COLUMNS.keys())
    unknown_tables = [t for t in target_tables if t not in TABLE_DDL_COLUMNS]
    if unknown_tables:
        raise ValueError(
            f"Bảng '{unknown_tables[0]}' chưa được khai báo DDL trong TABLE_DDL_COLUMNS."
        )

    updated_counts = {}
    excluded_cols = (
        list(exclude_meta_columns)
        if exclude_meta_columns is not None
        else list(BACKFILL_EXCLUDED_META_COLUMNS)
    )
    where_clause = sql.SQL(" WHERE tbl.raw_payload IS NULL") if only_null else sql.SQL("")

    with conn.transaction():
        with conn.cursor() as cur:
            for table_name in target_tables:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                    (schema, table_name),
                )
                if cur.fetchone() is None:
                    updated_counts[table_name] = 0
                    continue
                ensure_raw_table(cur, schema, table_name)
                cur.execute(
                    sql.SQL(
                        "UPDATE {} AS tbl "
                        "SET raw_payload = to_jsonb(tbl) - %s::text[]{}"
                    ).format(table_ref(schema, table_name), where_clause),
                    (excluded_cols,),
                )
                rowcount = cur.rowcount
                updated_counts[table_name] = (
                    rowcount
                    if isinstance(rowcount, int) and not isinstance(rowcount, bool) and rowcount >= 0
                    else 0
                )
    return updated_counts
