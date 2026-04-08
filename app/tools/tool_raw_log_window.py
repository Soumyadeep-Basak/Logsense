"""Helpers for extracting raw log windows from parsed logs."""

from __future__ import annotations

import pandas as pd
try:
    from langchain_core.tools import tool
except ImportError:  # pragma: no cover - compatibility fallback
    from langchain.tools import tool

from app.tools._tool_data_loader import load_parsed_logs


def _get_raw_log_window_impl(
    start_row: int,
    end_row: int,
    process_name: str | None = None,
) -> list[str]:
    """Return raw log lines for a row window with optional process-name filtering."""
    parsed = load_parsed_logs()
    if parsed.empty:
        return []

    window = _slice_window(parsed, start_row, end_row)
    if process_name:
        process_column = _resolve_process_column(window)
        if process_column is None:
            return []
        window = window[
            window[process_column].astype(str).str.lower() == str(process_name).lower()
        ]

    return _extract_text_lines(window)


def _slice_window(parsed: pd.DataFrame, start_row: int, end_row: int) -> pd.DataFrame:
    if "row_id" in parsed.columns:
        return parsed[(parsed["row_id"] >= start_row) & (parsed["row_id"] <= end_row)]
    return parsed.iloc[start_row : end_row + 1]


def _resolve_process_column(df: pd.DataFrame) -> str | None:
    for column in ("process", "process_name"):
        if column in df.columns:
            return column
    return None


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


@tool
def get_raw_log_window(
    start_row: int,
    end_row: int,
    process_name: str | None = None,
) -> list[str]:
    """Fetch raw log lines for a row window with optional process filtering.

    Use this when the LLM knows an approximate row range and wants the exact
    source log lines, optionally limited to one process within that window.
    Returns a list of raw log strings in the requested window.
    """
    return _get_raw_log_window_impl(start_row=start_row, end_row=end_row, process_name=process_name)


if __name__ == "__main__":
    print(ascii(_get_raw_log_window_impl(0, 5)))
