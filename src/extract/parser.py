import json
import pandas as pd
import pyarrow as pa

INDICATORS = (
    "NY.GDP.MKTP.CD",
    "NY.GDP.MKTP.KD.ZG",
    "FP.CPI.TOTL.ZG",
    "SP.POP.TOTL",
    "SP.DYN.LE00.IN",
    "SL.UEM.TOTL.ZS",
    "NE.TRD.GNFS.ZS",
    "IT.NET.USER.ZS",
)

INGESTION_META_COLUMNS = (
    "extracted_at",
    "batch_id",
    "source_endpoint",
    "ingestion_run_id",
)


def make_schema(fields):
    pa_fields = [(item, pa.string()) if isinstance(item, str) else item for item in fields]
    pa_fields.append(("raw_payload", pa.string()))
    pa_fields.extend((col, pa.string()) for col in INGESTION_META_COLUMNS)
    return pa.schema(pa_fields)


def serialize_raw_payload(row):
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str)


def parse_optional_year(raw_date):
    if raw_date is None or isinstance(raw_date, bool):
        return None
    text = str(raw_date).strip()
    if len(text) == 4 and text.isascii() and text.isdigit():
        return int(text)
    return None


PARQUET_SCHEMAS = {
    "raw_worldbank_indicators": make_schema(
        [
            "country_iso3",
            "country_iso2",
            "country_name",
            "indicator_code",
            "indicator_name",
            ("year", pa.int64()),
            ("value", pa.float64()),
            "unit",
            "obs_status",
            ("decimal", pa.int64()),
            "source_id",
            "last_updated",
        ]
    ),
    "raw_worldbank_countries": make_schema(
        [
            "country_iso3",
            "country_iso2",
            "country_name",
            "region_id",
            "region_iso2",
            "region_name",
            "admin_region_id",
            "admin_region_name",
            "income_level_id",
            "income_level_name",
            "lending_type_id",
            "lending_type_name",
            "capital_city",
            "longitude",
            "latitude",
        ]
    ),
    "raw_worldbank_indicator_metadata": make_schema(
        [
            "indicator_code",
            "indicator_name",
            "unit",
            "source_id",
            "source_name",
            "source_note",
            "source_organization",
            "topics",
        ]
    ),
}

INDICATOR_COLUMNS = tuple(PARQUET_SCHEMAS["raw_worldbank_indicators"].names)
COUNTRY_COLUMNS = tuple(PARQUET_SCHEMAS["raw_worldbank_countries"].names)
INDICATOR_META_COLUMNS = tuple(PARQUET_SCHEMAS["raw_worldbank_indicator_metadata"].names)


def chunked(records_iter, chunk_size):
    if chunk_size < 1:
        raise ValueError(f"chunk_size phải >= 1, giá trị hiện tại: {chunk_size}.")

    batch = []
    for record in records_iter:
        batch.append(record)
        if len(batch) >= chunk_size:
            yield batch
            batch = []

    if batch:
        yield batch


def meta_fields(extracted_at="", batch_id="", source_endpoint="", ingestion_run_id=""):
    return {
        "extracted_at": extracted_at,
        "batch_id": batch_id,
        "source_endpoint": source_endpoint,
        "ingestion_run_id": ingestion_run_id,
    }


def parse_date_range(date_range):
    raw = str(date_range or "").strip()
    if not raw:
        raise ValueError("WORLD_BANK_DATE_RANGE không được để trống.")

    parts = raw.split(":")
    try:
        if len(parts) == 1:
            start_year = end_year = int(parts[0])
        elif len(parts) == 2:
            start_year = int(parts[0])
            end_year = int(parts[1])
        else:
            raise ValueError(
                f"Khoảng năm không hợp lệ: '{raw}'. Định dạng đúng là 'YYYY:YYYY' hoặc 'YYYY'."
            )
    except ValueError as err:
        raise ValueError(f"Khoảng năm không hợp lệ: '{raw}'.") from err

    if start_year > end_year:
        raise ValueError(
            f"Khoảng năm không hợp lệ: '{raw}' (năm bắt đầu > năm kết thúc)."
        )
    return start_year, end_year


def records_to_dataframe(
    table_name,
    records,
    *,
    extracted_at="",
    batch_id="",
    source_endpoint="",
    ingestion_run_id="",
):
    if table_name not in PARQUET_SCHEMAS:
        raise ValueError(f"Bảng '{table_name}' chưa được khai báo schema trong PARQUET_SCHEMAS.")

    expected_cols = list(PARQUET_SCHEMAS[table_name].names)
    df = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(list(records))
    if df.empty:
        df = pd.DataFrame(columns=expected_cols)

    defaults = meta_fields(extracted_at, batch_id, source_endpoint, ingestion_run_id)
    for col, default_val in defaults.items():
        if col not in df.columns:
            df[col] = default_val
        else:
            series = df[col].astype("string")
            df[col] = series.where(series.notna() & (series != ""), default_val)

    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[expected_cols]
    if table_name == "raw_worldbank_indicators":
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df["decimal"] = pd.to_numeric(df["decimal"], errors="coerce").astype("Int64")
        df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("Float64")

    return df


def flatten_indicator_row(
    row,
    meta,
    fallback_indicator,
    *,
    extracted_at="",
    batch_id="",
    source_endpoint="",
    ingestion_run_id="",
):
    country = row.get("country") or {}
    indicator = row.get("indicator") or {}
    raw_date = row.get("date")
    country_iso3 = str(row.get("countryiso3code") or "").strip()
    country_iso2 = str(country.get("id") or "").strip() or country_iso3
    indicator_code = str(indicator.get("id") or fallback_indicator or "").strip()
    return {
        "country_iso3": country_iso3,
        "country_iso2": country_iso2,
        "country_name": country.get("value") or "",
        "indicator_code": indicator_code,
        "indicator_name": indicator.get("value") or "",
        "year": parse_optional_year(raw_date),
        "value": row.get("value"),
        "unit": row.get("unit") or "",
        "obs_status": row.get("obs_status") or "",
        "decimal": row.get("decimal"),
        "source_id": meta.get("sourceid") or "",
        "last_updated": meta.get("lastupdated") or "",
        "raw_payload": serialize_raw_payload(row),
        **meta_fields(extracted_at, batch_id, source_endpoint, ingestion_run_id),
    }


def flatten_country_row(
    row,
    *,
    extracted_at="",
    batch_id="",
    source_endpoint="",
    ingestion_run_id="",
):
    region = row.get("region") or {}
    admin_region = row.get("adminregion") or {}
    income_level = row.get("incomeLevel") or {}
    lending_type = row.get("lendingType") or {}
    return {
        "country_iso3": row.get("id") or "",
        "country_iso2": row.get("iso2Code") or "",
        "country_name": row.get("name") or "",
        "region_id": region.get("id") or "",
        "region_iso2": region.get("iso2code") or "",
        "region_name": region.get("value") or "",
        "admin_region_id": admin_region.get("id") or "",
        "admin_region_name": admin_region.get("value") or "",
        "income_level_id": income_level.get("id") or "",
        "income_level_name": income_level.get("value") or "",
        "lending_type_id": lending_type.get("id") or "",
        "lending_type_name": lending_type.get("value") or "",
        "capital_city": row.get("capitalCity") or "",
        "longitude": row.get("longitude") or "",
        "latitude": row.get("latitude") or "",
        "raw_payload": serialize_raw_payload(row),
        **meta_fields(extracted_at, batch_id, source_endpoint, ingestion_run_id),
    }


def flatten_indicator_meta_row(
    row,
    fallback_indicator,
    *,
    extracted_at="",
    batch_id="",
    source_endpoint="",
    ingestion_run_id="",
):
    source = row.get("source") or {}
    raw_topics = row.get("topics") or []
    topics = " | ".join(
        topic["value"] for topic in raw_topics if isinstance(topic, dict) and topic.get("value")
    )
    return {
        "indicator_code": row.get("id") or fallback_indicator,
        "indicator_name": row.get("name") or "",
        "unit": row.get("unit") or "",
        "source_id": source.get("id") or "",
        "source_name": source.get("value") or "",
        "source_note": row.get("sourceNote") or "",
        "source_organization": row.get("sourceOrganization") or "",
        "topics": topics,
        "raw_payload": serialize_raw_payload(row),
        **meta_fields(extracted_at, batch_id, source_endpoint, ingestion_run_id),
    }
