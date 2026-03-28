from pathlib import Path

import pandas as pd

from app.config import CHUNKS_PATH

BASE_WINDOW = 3
OUTPUT_COLUMNS = [
    "chunk_id",
    "start_row",
    "end_row",
    "process_pid",
    "process_name",
    "description",
]


def build_chunks(input_csv: str | Path, output_csv: str | Path = CHUNKS_PATH) -> None:
    """Build incident chunks around anomaly rows and persist them to CSV."""
    logs = pd.read_csv(input_csv)
    index_column = _resolve_index_column(logs)
    logs = _normalize_logs(logs, index_column)

    chunks = _build_raw_chunks(logs)
    merged_chunks = _merge_overlapping_chunks(chunks)
    output = _format_chunks(merged_chunks)

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_path, index=False)


def _resolve_index_column(logs: pd.DataFrame) -> str:
    for candidate in ("row_id", "row_no"):
        if candidate in logs.columns:
            return candidate
    raise KeyError("Missing required row index column. Expected one of: row_id, row_no")


def _normalize_logs(logs: pd.DataFrame, index_column: str) -> pd.DataFrame:
    required_columns = {"process_name", "process_pid", "is_anomaly"}
    missing_columns = required_columns.difference(logs.columns)
    if missing_columns:
        raise KeyError(f"Missing columns: {sorted(missing_columns)}")

    normalized = logs.copy()
    normalized[index_column] = pd.to_numeric(normalized[index_column], errors="coerce")
    normalized = normalized.dropna(subset=[index_column]).copy()
    normalized[index_column] = normalized[index_column].astype(int)

    normalized["is_anomaly"] = normalized["is_anomaly"].apply(_to_bool)
    normalized["process_pid"] = pd.to_numeric(normalized["process_pid"], errors="coerce")
    normalized["process_name"] = normalized["process_name"].fillna("").astype(str)

    normalized = normalized.sort_values(index_column).reset_index(drop=True)
    normalized["row_index"] = normalized[index_column]
    return normalized


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


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
                "process_pid": chunk["process_pid"],
                "process_name": chunk["process_name"],
                "description": chunk["description"],
            }
        )

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
