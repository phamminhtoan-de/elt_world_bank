from src.extract.parser import parse_date_range


def plan_incremental_tasks(
    task_specs,
    loaded_tasks,
    watermarks,
    *,
    date_range,
    current_api_updated="",
    lookback_years=3,
    base_load_mode="append",
):
    start_year, end_year = parse_date_range(date_range)
    selected_specs = []
    task_overrides = {}
    meta_spec = None
    incremental_mode = "append" if base_load_mode == "append" else "upsert"

    for spec in task_specs:
        task_key, table_name, _, _ = spec
        if table_name == "raw_worldbank_indicator_metadata":
            meta_spec = spec
            continue

        if task_key in watermarks:
            wm = watermarks[task_key]
            has_api_update = bool(
                current_api_updated
                and current_api_updated > (wm.get("api_last_updated") or "")
            )
            if table_name == "raw_worldbank_indicators":
                last_year = wm.get("last_loaded_year")
                has_newer_year = last_year is None or end_year > last_year
                if not has_newer_year and not has_api_update:
                    continue
                inc_start = (
                    max(start_year, last_year - lookback_years)
                    if last_year is not None
                    else start_year
                )
                task_overrides[task_key] = {
                    "date_range": f"{inc_start}:{end_year}",
                    "load_mode": incremental_mode,
                }
                selected_specs.append(spec)
            elif has_api_update:
                selected_specs.append(spec)
        elif task_key not in loaded_tasks:
            selected_specs.append(spec)

    if meta_spec is not None:
        meta_key = meta_spec[0]
        has_indicator_work = any(
            spec[1] == "raw_worldbank_indicators" for spec in selected_specs
        )
        meta_wm = watermarks.get(meta_key)
        has_meta_api_update = bool(
            meta_wm
            and current_api_updated
            and current_api_updated > (meta_wm.get("api_last_updated") or "")
        )
        meta_missing = meta_key not in watermarks and meta_key not in loaded_tasks
        if has_indicator_work or meta_missing or has_meta_api_update:
            insert_idx = (
                1
                if selected_specs and selected_specs[0][1] == "raw_worldbank_countries"
                else 0
            )
            selected_specs.insert(insert_idx, meta_spec)

    return tuple(selected_specs), task_overrides
