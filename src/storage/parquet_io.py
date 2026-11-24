from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.logger import get_logger
from src.config.settings import settings
from src.extract.parser import (
    INGESTION_META_COLUMNS,
    PARQUET_SCHEMAS,
    chunked,
    records_to_dataframe,
)
from src.storage.database import TABLE_DDL_COLUMNS

logger = get_logger(__name__)


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


def read_parquet_batch_id(parquet_file):
    schema_meta = parquet_file.schema_arrow.metadata or {}
    raw_meta_batch = schema_meta.get(b"batch_id")
    if raw_meta_batch:
        meta_batch = raw_meta_batch.decode("utf-8", errors="ignore").strip()
        if meta_batch:
            return meta_batch
    if (
        "batch_id" not in parquet_file.schema_arrow.names
        or parquet_file.metadata.num_rows == 0
        or parquet_file.metadata.num_row_groups == 0
    ):
        return None
    first_group = parquet_file.read_row_group(0, columns=["batch_id"])
    for value in first_group.column("batch_id").to_pylist():
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def read_parquet_file_batch_id(parquet_path):
    path = Path(parquet_path)
    if not path.exists():
        return None
    try:
        parquet_file = pq.ParquetFile(path)
        try:
            return read_parquet_batch_id(parquet_file)
        finally:
            del parquet_file
    except Exception:
        return None


def cleanup_stale_temp_parquet_files(target_dir):
    directory = Path(target_dir)
    if not directory.exists():
        return ()
    removed = []
    for tmp_file in sorted(directory.glob("*.parquet.tmp")):
        try:
            tmp_file.unlink()
            removed.append(tmp_file)
            logger.warning("[CLEANUP TMP] Đã xóa file Parquet tạm còn sót: %s", tmp_file)
        except OSError as err:
            logger.warning("Không thể xóa file tạm %s: %s", tmp_file, err)
    return tuple(removed)


def validate_parquet_file(
    parquet_path, expected_row_count=None, allow_empty=True, expected_batch_id=None
):
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file Parquet để kiểm tra Data Quality: {path}")

    parquet_file = pq.ParquetFile(path)
    num_rows = parquet_file.metadata.num_rows
    col_names = set(parquet_file.schema_arrow.names)
    actual_batch_id = read_parquet_batch_id(parquet_file) if expected_batch_id else None
    del parquet_file

    if not allow_empty and num_rows <= 0:
        raise ValueError(f"Data Quality lỗi: file Parquet '{path.name}' rỗng (0 dòng).")
    if expected_row_count is not None and num_rows != expected_row_count:
        raise ValueError(
            f"Data Quality lỗi tại '{path.name}': số dòng file ({num_rows}) không khớp ({expected_row_count})."
        )

    missing_meta = [col for col in INGESTION_META_COLUMNS if col not in col_names]
    if missing_meta:
        raise ValueError(f"Data Quality lỗi tại '{path.name}': thiếu cột metadata {missing_meta}.")

    if expected_batch_id and (actual_batch_id is not None or num_rows > 0):
        if actual_batch_id != expected_batch_id:
            raise ValueError(
                f"Data Quality lỗi tại '{path.name}': batch_id file ({actual_batch_id}) không khớp ({expected_batch_id})."
            )
    return num_rows


def write_records_to_parquet(
    table_name,
    records_iter,
    output_dir=None,
    chunk_size=None,
    batch_id="",
    source_endpoint="",
    ingestion_run_id="",
    extracted_at="",
    progress=None,
    file_stem=None,
    allow_empty=True,
):
    if table_name not in PARQUET_SCHEMAS:
        raise ValueError(f"Bảng '{table_name}' chưa được khai báo schema trong PARQUET_SCHEMAS.")

    base_schema = PARQUET_SCHEMAS[table_name]
    if batch_id:
        existing_meta = dict(base_schema.metadata or {})
        existing_meta[b"batch_id"] = str(batch_id).encode("utf-8")
        schema = base_schema.with_metadata(existing_meta)
    else:
        schema = base_schema
    effective_chunk_size = chunk_size if chunk_size is not None else settings.elt_chunk_size
    effective_extracted_at = extracted_at or datetime.now(timezone.utc).isoformat()
    target_dir = Path(output_dir) if output_dir is not None else settings.raw_data_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    stem = file_stem or table_name
    output_path = target_dir / f"{stem}.parquet"
    tmp_path = target_dir / f"{stem}.parquet.tmp"
    total_rows = 0
    chunk_count = 0

    try:
        with pq.ParquetWriter(tmp_path, schema) as writer:
            for chunk in chunked(records_iter, effective_chunk_size):
                df_chunk = records_to_dataframe(
                    table_name,
                    chunk,
                    extracted_at=effective_extracted_at,
                    batch_id=batch_id,
                    source_endpoint=source_endpoint,
                    ingestion_run_id=ingestion_run_id,
                )
                writer.write_table(
                    pa.Table.from_pandas(df_chunk, schema=schema, preserve_index=False)
                )
                total_rows += len(df_chunk)
                chunk_count += 1
                logger.info(
                    "  -> %s | Ghi chunk %d (+%d dòng, tổng %s dòng)",
                    stem,
                    chunk_count,
                    len(df_chunk),
                    f"{total_rows:,}",
                )

            if total_rows == 0:
                empty_df = records_to_dataframe(
                    table_name,
                    [],
                    extracted_at=effective_extracted_at,
                    batch_id=batch_id,
                    source_endpoint=source_endpoint,
                    ingestion_run_id=ingestion_run_id,
                )
                writer.write_table(
                    pa.Table.from_pandas(empty_df, schema=schema, preserve_index=False)
                )

        validate_parquet_file(
            tmp_path,
            expected_row_count=total_rows,
            allow_empty=allow_empty,
            expected_batch_id=batch_id or None,
        )
        tmp_path.replace(output_path)
    except BaseException:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    progress_state = progress or {}
    return SimpleNamespace(
        table_name=table_name,
        path=output_path,
        columns=tuple(schema.names),
        row_count=total_rows,
        batch_id=batch_id,
        source_endpoint=source_endpoint,
        ingestion_run_id=ingestion_run_id,
        extracted_at=effective_extracted_at,
        last_extracted_page=progress_state.get("last_page", 0),
        last_checkpoint_key=progress_state.get("last_key", ""),
    )


def iter_parquet_dataframe_chunks(parquet_path, chunk_size=None):
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file Parquet: {path}")

    effective_chunk_size = chunk_size if chunk_size is not None else settings.elt_chunk_size
    if effective_chunk_size < 1:
        raise ValueError(f"chunk_size phải >= 1, giá trị hiện tại: {effective_chunk_size}.")

    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=effective_chunk_size):
        yield batch.to_pandas(types_mapper={pa.int64(): pd.Int64Dtype()}.get)
