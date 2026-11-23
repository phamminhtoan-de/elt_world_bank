from psycopg import sql

from src.storage.database import TABLE_PRIMARY_KEYS, table_ref


def validate_loaded_batch_in_cur(cur, *, table_name, expected_rows, rows_loaded):
    if expected_rows is not None and rows_loaded != expected_rows:
        raise ValueError(
            f"Data Quality lỗi tại bảng '{table_name}': số dòng nạp vào DB ({rows_loaded}) không khớp số dòng nguồn ({expected_rows})."
        )


def upsert_from_temp_table(cur, schema, table_name, temp_table, insert_columns):
    if table_name not in TABLE_PRIMARY_KEYS:
        raise ValueError(f"Bảng '{table_name}' chưa được khai báo khóa chính trong TABLE_PRIMARY_KEYS.")

    target_ref = table_ref(schema, table_name)
    pk_cols = TABLE_PRIMARY_KEYS[table_name]
    join_cond = sql.SQL(" AND ").join(
        sql.SQL("tgt.{} IS NOT DISTINCT FROM stg.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in pk_cols
    )
    cur.execute(
        sql.SQL("DELETE FROM {} AS tgt USING {} AS stg WHERE {}").format(
            target_ref, sql.Identifier(temp_table), join_cond
        )
    )

    col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in insert_columns)
    pk_ids = sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols)
    order_clause = (
        sql.SQL("{}, extracted_at DESC NULLS LAST").format(pk_ids) if "extracted_at" in insert_columns else pk_ids
    )
    cur.execute(
        sql.SQL("INSERT INTO {} ({}) SELECT DISTINCT ON ({}) {} FROM {} ORDER BY {}").format(
            target_ref, col_ids, pk_ids, col_ids, sql.Identifier(temp_table), order_clause
        )
    )


def prepare_copy_target(cur, schema, table_name, load_mode, indicator_code):
    target_ref = table_ref(schema, table_name)
    if load_mode == "replace":
        if indicator_code:
            cur.execute(sql.SQL("DELETE FROM {} WHERE indicator_code = %s").format(target_ref), (indicator_code,))
        else:
            cur.execute(sql.SQL("TRUNCATE TABLE {}").format(target_ref))
        return target_ref, None
    if load_mode == "append":
        return target_ref, None

    temp_table = f"tmp_stg_{table_name}"
    cur.execute(
        sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
            sql.Identifier(temp_table), target_ref
        )
    )
    return sql.Identifier(temp_table), temp_table


def batch_already_loaded_in_cur(cur, schema, table_name, batch_id, indicator_code=None):
    if not batch_id:
        return False
    if indicator_code:
        cur.execute(
            sql.SQL(
                "SELECT EXISTS (SELECT 1 FROM {} WHERE batch_id = %s AND indicator_code = %s)"
            ).format(table_ref(schema, table_name)),
            (batch_id, indicator_code),
        )
    else:
        cur.execute(
            sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE batch_id = %s)").format(
                table_ref(schema, table_name)
            ),
            (batch_id,),
        )
    row = cur.fetchone()
    return bool(row and row[0] is True)
