from datetime import datetime, timezone
from pathlib import Path
from psycopg import sql

from src.config.logger import get_logger, log_step
from src.config.settings import VALID_LOAD_MODES, settings
from src.extract.parser import parse_date_range
from src.pipeline.load_stage import run_load_stage
from src.reliability.checkpoints import record_load_failure
from src.reliability.recovery import build_failure_summary, mark_run_failed
from src.reliability.runs import create_or_resume_run, get_resumable_run_id
from src.reliability.schema import CHECKPOINTS_TABLE, ensure_state_schema
from src.storage.database import open_db_connection, table_ref
from src.storage.parquet_io import discover_parquet_files, read_parquet_file_batch_id

logger = get_logger("elt.pipeline")

TABLE_LOAD_PRIORITY = {
    "raw_worldbank_countries": 0,
    "raw_worldbank_indicator_metadata": 1,
    "raw_worldbank_indicators": 2,
}


def run_load_only(
    *,
    data_dir=None,
    load_mode=None,
    schema=None,
    state_schema=None,
    chunk_size=None,
    auto_resume=None,
    fresh=None,
    run_id=None,
    indicators=None,
    cfg=None,
):
    active_settings = cfg or settings
    base_data_dir = Path(data_dir) if data_dir is not None else active_settings.raw_data_dir
    effective_mode = (load_mode or active_settings.elt_load_mode).lower()
    if effective_mode not in VALID_LOAD_MODES:
        raise ValueError(
            f"load_mode không hợp lệ: '{effective_mode}'. Chỉ chấp nhận {sorted(VALID_LOAD_MODES)}."
        )

    target_schema = schema or active_settings.tgt_db_schema
    effective_state_schema = state_schema or active_settings.state_db_schema
    effective_chunk_size = chunk_size if chunk_size is not None else active_settings.elt_chunk_size
    effective_fresh = active_settings.elt_fresh if fresh is None else fresh
    effective_auto_resume = (
        (False if effective_fresh else active_settings.elt_auto_resume)
        if auto_resume is None
        else auto_resume
    )
    effective_run_id = run_id if run_id is not None else active_settings.elt_run_id
    _, config_end_year = parse_date_range(active_settings.world_bank_date_range)
    indicator_filter = set(indicators) if indicators is not None else None

    parquet_paths = {}
    missing_failures = []

    with log_step(logger, f"Chế độ LOAD-ONLY (không gọi API) | state_schema='{effective_state_schema}'"):
        with open_db_connection() as conn:
            ensure_state_schema(conn, effective_state_schema)
            resumable_run_id = (
                get_resumable_run_id(conn, effective_state_schema, run_id=effective_run_id)
                if (effective_auto_resume or effective_run_id)
                else None
            )
            if resumable_run_id:
                ingestion_run_id, _, checkpoints = create_or_resume_run(
                    conn,
                    effective_state_schema,
                    table_specs=(),
                    load_mode=effective_mode,
                    target_schema=target_schema,
                    data_dir=base_data_dir,
                    chunk_size=effective_chunk_size,
                    auto_resume=effective_auto_resume,
                    run_id=effective_run_id,
                )
                ordered_cps = sorted(
                    checkpoints.values(),
                    key=lambda cp: (TABLE_LOAD_PRIORITY.get(cp["table_name"], 9), cp["task_key"]),
                )
                specs = []
                for cp in ordered_cps:
                    if (
                        indicator_filter is not None
                        and cp["table_name"] == "raw_worldbank_indicators"
                        and cp.get("indicator_code") not in indicator_filter
                    ):
                        continue
                    tkey = cp["task_key"]
                    tbl = cp["table_name"]
                    specs.append((tkey, tbl, cp.get("indicator_code"), cp.get("source_endpoint") or ""))
                    if cp.get("load_status") == "LOADED":
                        continue
                    candidate = (
                        Path(cp["parquet_path"])
                        if cp.get("parquet_path")
                        else (Path(cp.get("run_data_dir") or base_data_dir) / f"{tkey}.parquet")
                    )
                    if candidate.exists():
                        parquet_paths[tkey] = candidate
                    elif tbl != "raw_worldbank_indicators":
                        raise FileNotFoundError(f"Không tìm thấy file Parquet cho bảng nền {tkey}: {candidate}")
                    else:
                        msg = f"Thiếu file Parquet cho {tkey}: {candidate}"
                        missing_failures.append((tkey, msg))
                        now = datetime.now(timezone.utc)
                        record_load_failure(
                            conn,
                            effective_state_schema,
                            ingestion_run_id=ingestion_run_id,
                            table_name=tbl,
                            batch_id=cp["batch_id"],
                            parquet_path=candidate,
                            started_at=now,
                            finished_at=now,
                            duration_seconds=0.0,
                            error_message=msg,
                            task_key=tkey,
                        )
                effective_task_specs = tuple(specs)
            else:
                discovered = discover_parquet_files(base_data_dir)
                filtered = []
                for path in discovered:
                    parts = path.stem.split("__", 1)
                    tbl = parts[0]
                    code = parts[1] if len(parts) > 1 else None
                    if indicator_filter is not None and tbl == "raw_worldbank_indicators" and code not in indicator_filter:
                        continue
                    filtered.append((path, path.stem, tbl, code))
                if not filtered:
                    raise FileNotFoundError(f"Không tìm thấy file Parquet phù hợp trong {base_data_dir}")
                filtered.sort(key=lambda item: (TABLE_LOAD_PRIORITY.get(item[2], 9), item[1]))
                effective_task_specs = tuple(
                    (tkey, tbl, code, f"file://{path.name}") for path, tkey, tbl, code in filtered
                )
                ingestion_run_id, _, checkpoints = create_or_resume_run(
                    conn,
                    effective_state_schema,
                    table_specs=effective_task_specs,
                    load_mode=effective_mode,
                    target_schema=target_schema,
                    data_dir=base_data_dir,
                    chunk_size=effective_chunk_size,
                    auto_resume=False,
                    run_id=None,
                )
                cp_updates = []
                for path, tkey, _, _ in filtered:
                    cp = checkpoints[tkey]
                    embedded_batch = read_parquet_file_batch_id(path)
                    if embedded_batch:
                        cp["batch_id"] = embedded_batch
                    cp["parquet_path"] = str(path)
                    cp["extract_status"] = "EXTRACTED"
                    parquet_paths[tkey] = path
                    cp_updates.append((cp["batch_id"], str(path), ingestion_run_id, tkey))
                if cp_updates:
                    cp_ref = table_ref(effective_state_schema, CHECKPOINTS_TABLE)
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.executemany(
                                sql.SQL(
                                    "UPDATE {} SET batch_id = %s, parquet_path = %s, extract_status = 'EXTRACTED', updated_at = CURRENT_TIMESTAMP WHERE ingestion_run_id = %s AND task_key = %s"
                                ).format(cp_ref),
                                cp_updates,
                            )

    try:
        with log_step(logger, f"ELT Load-Only Pipeline (ingestion_run_id={ingestion_run_id}, files={len(parquet_paths)})"):
            results, load_failures = run_load_stage(
                effective_task_specs,
                checkpoints,
                parquet_paths,
                {},
                target_schema=target_schema,
                effective_state_schema=effective_state_schema,
                effective_mode=effective_mode,
                effective_chunk_size=effective_chunk_size,
                config_end_year=config_end_year,
                current_api_updated="",
                ingestion_run_id=ingestion_run_id,
                active_settings=active_settings,
                has_extract_failures=bool(missing_failures),
            )
            all_failures = [*missing_failures, *load_failures]
            if all_failures:
                raise RuntimeError(build_failure_summary(all_failures))
            return results
    except Exception as err:
        mark_run_failed(effective_state_schema, ingestion_run_id, str(err))
        raise
