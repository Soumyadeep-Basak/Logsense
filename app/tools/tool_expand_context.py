"""Helpers for expanding an incident into raw log context."""

from __future__ import annotations

from typing import Any

import pandas as pd

from app.tools._tool_data_loader import load_final, load_parsed_logs


def expand_incident_context(chunk_id: str) -> dict[str, Any]:
    """Return descriptions and raw log lines for a given incident chunk id."""
    df = load_final()
    if df.empty or "chunk_id" not in df.columns:
        return {"error": f"chunk_id {chunk_id} not found"}

    matches = df[df["chunk_id"].astype(str) == str(chunk_id)]
    if matches.empty:
        return {"error": f"chunk_id {chunk_id} not found"}

    row = matches.iloc[0]
    start_row = _safe_int(row.get("start_row"))
    end_row = _safe_int(row.get("end_row"))

    parsed = load_parsed_logs()
    window = _slice_window(parsed, start_row, end_row)
    raw_lines = _extract_text_lines(window)

    return {
        "chunk_id": str(chunk_id),
        "high_level_description": str(row.get("high_level_description", "")),
        "process_name": str(row.get("process_name", "")),
        "process_pid": str(row.get("process_pid", "")),
        "start_row": start_row,
        "end_row": end_row,
        "raw_log_lines": raw_lines,
        "line_count": len(raw_lines),
    }


def _slice_window(parsed: pd.DataFrame, start_row: int, end_row: int) -> pd.DataFrame:
    if parsed.empty:
        return parsed
    if "row_id" in parsed.columns:
        return parsed[(parsed["row_id"] >= start_row) & (parsed["row_id"] <= end_row)]
    return parsed.iloc[start_row : end_row + 1]


def _extract_text_lines(window: pd.DataFrame) -> list[str]:
    if window.empty:
        return []
    if "logline" in window.columns:
        return [str(value) for value in window["logline"].fillna("").tolist()]

    for column in window.columns:
        series = window[column]
        if pd.api.types.is_string_dtype(series) or series.dtype == object:
            return [str(value) for value in series.fillna("").tolist()]
    return []


def _safe_int(value: object) -> int:
    numeric = pd.to_numeric(value, errors="coerce")
    return int(numeric) if pd.notna(numeric) else 0


if __name__ == "__main__":
    print(ascii(expand_incident_context("chunk_0")))
