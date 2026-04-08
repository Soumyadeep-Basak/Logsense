from pathlib import Path

import pandas as pd

from app.config import CHUNKS_PATH, FINAL_PATH, PARSED_LOGS_PATH

BASE_WINDOW = 3
CRITICAL_PROCESSES = {"sshd", "kernel", "systemd", "cron"}
OUTPUT_COLUMNS = [
    "chunk_id",
    "start_row",
    "end_row",
    "process_name",
    "process_pid",
    "description",
    "anomaly_flag",
    "anomaly_count",
    "pid_count",
    "dominant_pid",
    "process_frequency",
    "is_critical_process",
    "recency_score",
    "log_density",
    "template_diversity",
    "risk_score",
]


def build_chunks(input_csv: str | Path, output_csv: str | Path = CHUNKS_PATH) -> None:
    """Build incident chunks around anomaly rows and persist them to CSV."""
    anomaly_logs = pd.read_csv(input_csv)
    anomaly_index_column = _resolve_index_column(anomaly_logs)
    anomaly_logs = _normalize_logs(anomaly_logs, anomaly_index_column)

    parsed_logs = pd.read_csv(PARSED_LOGS_PATH)
    parsed_index_column = _resolve_index_column(parsed_logs)
    parsed_logs = _normalize_logs(parsed_logs, parsed_index_column)
    parsed_logs = _attach_anomaly_flags(parsed_logs, anomaly_logs)

    chunks = _build_raw_chunks(anomaly_logs)
    merged_chunks = _merge_overlapping_chunks(chunks)
    output = _format_chunks(merged_chunks)
    output = _attach_chunk_features(output, parsed_logs)

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_path, index=False)

    final_output_path = Path(FINAL_PATH)
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(final_output_path, index=False)


def _resolve_index_column(logs: pd.DataFrame) -> str:
    for candidate in ("row_id", "row_no"):
        if candidate in logs.columns:
            return candidate
    raise KeyError("Missing required row index column. Expected one of: row_id, row_no")


def _normalize_logs(logs: pd.DataFrame, index_column: str) -> pd.DataFrame:
    required_columns = {"process_name", "process_pid"}
    missing_columns = required_columns.difference(logs.columns)
    if missing_columns:
        raise KeyError(f"Missing columns: {sorted(missing_columns)}")

    normalized = logs.copy()
    normalized[index_column] = pd.to_numeric(normalized[index_column], errors="coerce")
    normalized = normalized.dropna(subset=[index_column]).copy()
    normalized[index_column] = normalized[index_column].astype(int)

    normalized["is_anomaly"] = _get_series(normalized, "is_anomaly", False).apply(_to_bool)
    normalized["process_pid"] = pd.to_numeric(normalized["process_pid"], errors="coerce")
    normalized["process_name"] = normalized["process_name"].fillna("").astype(str)
    normalized["parsed_logline"] = _get_series(normalized, "parsed_logline", "").fillna("").astype(str)

    normalized = normalized.sort_values(index_column).reset_index(drop=True)
    normalized["row_index"] = normalized[index_column]
    normalized["event_timestamp"] = _build_event_timestamp(normalized)
    return normalized


def _get_series(frame: pd.DataFrame, column: str, default_value) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([default_value] * len(frame), index=frame.index)


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _build_event_timestamp(logs: pd.DataFrame) -> pd.Series:
    if "timestamp" in logs.columns:
        return pd.to_datetime(logs["timestamp"], errors="coerce")

    if {"month", "day", "time"}.issubset(logs.columns):
        timestamp_source = (
            "2000 "
            + logs["month"].fillna("").astype(str)
            + " "
            + logs["day"].fillna("").astype(str)
            + " "
            + logs["time"].fillna("").astype(str)
        )
        return pd.to_datetime(timestamp_source, format="%Y %b %d %H:%M:%S", errors="coerce")

    return pd.Series(pd.NaT, index=logs.index)


def _build_raw_chunks(logs: pd.DataFrame) -> list[dict[str, int | float | str]]:
    if logs.empty:
        return []

    row_indices = logs["row_index"].tolist()
    row_positions = {row_index: position for position, row_index in enumerate(row_indices)}
    anomaly_rows = logs.loc[logs["is_anomaly"]]

    chunks: list[dict[str, int | float | str]] = []
    max_position = len(logs) - 1

    for _, anomaly in anomaly_rows.iterrows():
        center_row = int(anomaly["row_index"])
        center_position = row_positions[center_row]

        start_position = max(0, center_position - BASE_WINDOW)
        end_position = min(max_position, center_position + BASE_WINDOW)

        # Re-scan the current window until no new anomaly or matching-PID rows extend it further.
        expanded = True
        while expanded:
            expanded = False
            window = logs.iloc[start_position : end_position + 1]
            matching_rows = window[
                window["is_anomaly"] | (window["process_pid"] == anomaly["process_pid"])
            ]

            if matching_rows.empty:
                continue

            new_start_position = row_positions[int(matching_rows["row_index"].min())]
            new_end_position = row_positions[int(matching_rows["row_index"].max())]

            if new_start_position < start_position or new_end_position > end_position:
                start_position = new_start_position
                end_position = new_end_position
                expanded = True

        chunks.append(
            {
                "start_row": int(logs.iloc[start_position]["row_index"]),
                "end_row": int(logs.iloc[end_position]["row_index"]),
                "process_pid": _serialize_pid(anomaly["process_pid"]),
                "process_name": anomaly["process_name"],
                "description": "",
            }
        )

    return chunks


def _serialize_pid(pid_value):
    if pd.isna(pid_value):
        return ""
    if float(pid_value).is_integer():
        return int(pid_value)
    return pid_value


def _attach_anomaly_flags(parsed_logs: pd.DataFrame, anomaly_logs: pd.DataFrame) -> pd.DataFrame:
    anomaly_flags = anomaly_logs[["row_index", "is_anomaly"]].copy()
    merged = parsed_logs.drop(columns=["is_anomaly"], errors="ignore").merge(
        anomaly_flags,
        on="row_index",
        how="left",
    )
    merged["is_anomaly"] = merged["is_anomaly"].fillna(False).apply(_to_bool)
    return merged


def _merge_overlapping_chunks(
    chunks: list[dict[str, int | float | str]],
) -> list[dict[str, int | float | str]]:
    if not chunks:
        return []

    sorted_chunks = sorted(chunks, key=lambda chunk: (chunk["start_row"], chunk["end_row"]))
    merged = [sorted_chunks[0].copy()]

    for chunk in sorted_chunks[1:]:
        current = merged[-1]
        if chunk["start_row"] <= current["end_row"]:
            current["end_row"] = max(current["end_row"], chunk["end_row"])
            continue
        merged.append(chunk.copy())

    return merged


def _format_chunks(
    chunks: list[dict[str, int | float | str]],
) -> pd.DataFrame:
    rows = []
    for index, chunk in enumerate(chunks, start=1):
        if chunk["start_row"] > chunk["end_row"]:
            continue

        rows.append(
            {
                "chunk_id": f"chunk_{index}",
                "start_row": chunk["start_row"],
                "end_row": chunk["end_row"],
                "process_name": chunk["process_name"],
                "process_pid": chunk["process_pid"],
                "description": chunk["description"],
            }
        )

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS[:6])


def _attach_chunk_features(chunks: pd.DataFrame, parsed_logs: pd.DataFrame) -> pd.DataFrame:
    if chunks.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    enriched = chunks.copy()
    process_counts = enriched["process_name"].fillna("").astype(str).value_counts().to_dict()
    chunk_timestamps: list[object] = []

    for row_number, chunk in enumerate(enriched.itertuples(index=False), start=1):
        chunk_rows = parsed_logs.loc[
            (parsed_logs["row_index"] >= int(chunk.start_row))
            & (parsed_logs["row_index"] <= int(chunk.end_row))
        ].copy()

        anomaly_count = int(chunk_rows["is_anomaly"].sum()) if not chunk_rows.empty else 0
        pid_series = chunk_rows["process_pid"].dropna() if not chunk_rows.empty else pd.Series(dtype=float)
        pid_count = int(pid_series.nunique()) if not pid_series.empty else 0

        if pid_series.empty:
            dominant_pid = ""
        else:
            dominant_pid = _serialize_pid(pid_series.value_counts().idxmax())

        template_diversity = (
            int(chunk_rows["parsed_logline"].fillna("").astype(str).replace("", pd.NA).dropna().nunique())
            if not chunk_rows.empty
            else 0
        )

        latest_timestamp = (
            chunk_rows["event_timestamp"].dropna().max()
            if not chunk_rows.empty and chunk_rows["event_timestamp"].notna().any()
            else pd.NaT
        )
        chunk_timestamps.append(latest_timestamp)

        enriched.at[row_number - 1, "anomaly_flag"] = int(anomaly_count > 0)
        enriched.at[row_number - 1, "anomaly_count"] = anomaly_count
        enriched.at[row_number - 1, "pid_count"] = pid_count
        enriched.at[row_number - 1, "dominant_pid"] = dominant_pid
        enriched.at[row_number - 1, "process_frequency"] = int(
            process_counts.get(str(chunk.process_name), 0)
        )
        enriched.at[row_number - 1, "is_critical_process"] = int(
            str(chunk.process_name).lower() in CRITICAL_PROCESSES
        )
        enriched.at[row_number - 1, "log_density"] = float(
            int(chunk.end_row) - int(chunk.start_row) + 1
        )
        enriched.at[row_number - 1, "template_diversity"] = template_diversity
        enriched.at[row_number - 1, "risk_score"] = None

    enriched["recency_score"] = _compute_recency_scores(chunk_timestamps)
    enriched["dominant_pid"] = enriched["dominant_pid"].apply(_serialize_maybe_numeric_string)
    enriched["anomaly_flag"] = enriched["anomaly_flag"].fillna(0).astype(int)
    enriched["anomaly_count"] = enriched["anomaly_count"].fillna(0).astype(int)
    enriched["pid_count"] = enriched["pid_count"].fillna(0).astype(int)
    enriched["process_frequency"] = enriched["process_frequency"].fillna(0).astype(int)
    enriched["is_critical_process"] = enriched["is_critical_process"].fillna(0).astype(int)
    enriched["log_density"] = enriched["log_density"].fillna(0.0).astype(float)
    enriched["template_diversity"] = enriched["template_diversity"].fillna(0).astype(int)
    enriched["recency_score"] = enriched["recency_score"].fillna(0.0).astype(float)

    return enriched.reindex(columns=OUTPUT_COLUMNS)


def _compute_recency_scores(chunk_timestamps: list[object]) -> pd.Series:
    timestamp_series = pd.Series(chunk_timestamps, dtype="datetime64[ns]")
    valid_timestamps = timestamp_series.dropna()
    if valid_timestamps.empty:
        return pd.Series([0.0] * len(timestamp_series), dtype=float)

    min_timestamp = valid_timestamps.min()
    max_timestamp = valid_timestamps.max()
    if min_timestamp == max_timestamp:
        scores = pd.Series([1.0] * len(timestamp_series), dtype=float)
        scores[timestamp_series.isna()] = 0.0
        return scores

    total_seconds = (max_timestamp - min_timestamp).total_seconds()
    scores = timestamp_series.apply(
        lambda value: 0.0
        if pd.isna(value)
        else float((value - min_timestamp).total_seconds() / total_seconds)
    )
    return scores.astype(float)


def _serialize_maybe_numeric_string(value) -> str:
    if pd.isna(value) or value == "":
        return ""
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return str(value)
    return str(_serialize_pid(numeric_value))
