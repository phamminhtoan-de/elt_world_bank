import pandas as pd

from src.extract.parser import INGESTION_META_COLUMNS, serialize_raw_payload

INT_SERIALIZE_COLUMNS = frozenset({"year", "decimal"})


def serialize_dataframe_chunk(df_or_records, columns, batch_id=None):
    df = df_or_records.copy() if isinstance(df_or_records, pd.DataFrame) else pd.DataFrame(list(df_or_records))
    if df.empty:
        return ""

    df = df.reindex(columns=list(columns))
    for col in INT_SERIALIZE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if batch_id is not None and "batch_id" not in columns:
        df["batch_id"] = batch_id

    return df.to_csv(index=False, header=False, na_rep="", lineterminator="\n")


def serialize_records_chunk(records, columns, batch_id=None):
    return serialize_dataframe_chunk(records, columns, batch_id=batch_id)


def attach_missing_raw_payload(chunk, source_columns):
    df = chunk.copy() if isinstance(chunk, pd.DataFrame) else pd.DataFrame(list(chunk))
    if "raw_payload" in df.columns:
        return df
    meta_cols = set(INGESTION_META_COLUMNS) | {"raw_payload", "ingested_at"}
    data_cols = [col for col in source_columns if col not in meta_cols and col in df.columns]
    records = df[data_cols].to_dict(orient="records") if data_cols else [{} for _ in range(len(df))]
    df["raw_payload"] = [
        serialize_raw_payload(
            {
                col: (None if pd.isna(val) else (val.item() if hasattr(val, "item") else val))
                for col, val in rec.items()
            }
        )
        for rec in records
    ]
    return df
