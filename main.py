import argparse

from src.config.settings import VALID_LOAD_MODES, load_settings, settings
from src.pipeline.runner import run_backfill_payload, run_ingestion


def parse_args():
    parser = argparse.ArgumentParser(
        description="World Bank ELT Ingestion: Extract Streaming API -> Parquet -> Load PostgreSQL (Bronze + State)."
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Đường dẫn file cấu hình YAML điều khiển pipeline (mặc định src/config/pipeline.yml).",
    )
    parser.add_argument(
        "--mode",
        choices=sorted(VALID_LOAD_MODES),
        default=None,
        help="Chế độ nạp dữ liệu vào DB (ghi đè pipeline.load_mode trong src/config/pipeline.yml).",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Schema PostgreSQL đích (ghi đè TGT_DB_SCHEMA trong .env).",
    )
    parser.add_argument(
        "--state-schema",
        default=None,
        help="Schema PostgreSQL quản lý state & checkpoint (ghi đè STATE_DB_SCHEMA trong .env).",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Thư mục lưu file Parquet tầng Raw (ghi đè pipeline.data_dir trong src/config/pipeline.yml).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Số dòng mỗi chunk khi ghi Parquet và COPY vào DB (ghi đè pipeline.chunk_size trong src/config/pipeline.yml).",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Tạo ingestion run mới thay vì tự động resume từ checkpoint lỗi/đang chạy (ghi đè pipeline.fresh / pipeline.auto_resume).",
    )
    parser.add_argument(
        "--backfill-payload",
        action="store_true",
        help="Chỉ chạy backfill cột raw_payload JSONB cho PostgreSQL theo cấu hình backfill.raw_payload trong src/config/pipeline.yml rồi kết thúc.",
    )
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Chỉ chạy giai đoạn Load các file Parquet sẵn có trên đĩa vào PostgreSQL (không gọi API World Bank).",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Chỉ định ingestion_run_id cụ thể để resume từ checkpoint (ghi đè pipeline.run_id trong src/config/pipeline.yml).",
    )
    parser.add_argument(
        "--source-id",
        default=None,
        help="Mã nguồn World Bank để lấy danh sách chỉ số (ghi đè WORLD_BANK_SOURCE_ID trong .env).",
    )
    parser.add_argument(
        "--max-indicators",
        type=int,
        default=None,
        help="Số lượng chỉ số tối đa tự động lấy từ API (0 để lấy toàn bộ, ghi đè WORLD_BANK_MAX_INDICATORS trong .env).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    active_settings = load_settings(args.config) if args.config else settings
    if args.backfill_payload and args.load_only:
        raise ValueError("Không thể bật đồng thời --backfill-payload và --load-only.")
    if args.backfill_payload or active_settings.backfill_raw_payload_enabled:
        run_backfill_payload(schema=args.schema, cfg=active_settings)
        return
    run_ingestion(
        data_dir=args.data_dir,
        load_mode=args.mode,
        schema=args.schema,
        state_schema=args.state_schema,
        chunk_size=args.chunk_size,
        auto_resume=False if args.fresh else None,
        fresh=True if args.fresh else None,
        run_id=args.run_id,
        source_id=args.source_id,
        max_indicators=args.max_indicators,
        load_only=True if args.load_only else None,
        cfg=active_settings,
    )


if __name__ == "__main__":
    main()
