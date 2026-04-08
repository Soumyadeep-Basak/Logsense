"""Recent-incident lookup helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd
try:
    from langchain_core.tools import tool
except ImportError:  # pragma: no cover - compatibility fallback
    from langchain.tools import tool

from app.tools._tool_data_loader import load_final


def _get_recent_incidents_impl(n: int = 1) -> list[dict[str, Any]]:
    """Return up to n most recent incidents as summary dictionaries."""
    df = load_final()
    if df.empty or n <= 0:
        return []

    sorted_df = _sort_incidents(df)
    rows = sorted_df.head(min(n, len(sorted_df)))
    return [_row_to_incident_dict(row) for _, row in rows.iterrows()]


def _sort_incidents(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" in df.columns:
        timestamps = pd.to_datetime(df["timestamp"], errors="coerce")
        if timestamps.notna().any():
            return df.assign(_tool_timestamp=timestamps).sort_values(
                by="_tool_timestamp",
                ascending=False,
                na_position="last",
            )
    return df.iloc[::-1]


def _row_to_incident_dict(row: pd.Series) -> dict[str, Any]:
    start_row = pd.to_numeric(row.get("start_row"), errors="coerce")
    end_row = pd.to_numeric(row.get("end_row"), errors="coerce")
    return {
        "chunk_id": str(row.get("chunk_id", "")),
        "process_name": str(row.get("process_name", "")),
        "process_pid": str(row.get("process_pid", "")),
        "description": str(row.get("description", "")),
        "start_row": int(start_row) if pd.notna(start_row) else 0,
        "end_row": int(end_row) if pd.notna(end_row) else 0,
        "timestamp": str(row.get("timestamp", "")) if "timestamp" in row.index else "",
    }


@tool
def get_recent_incidents(n: int = 1) -> list[dict[str, Any]]:
    """Get the most recent incident chunks from local Logsense data.

    Use this when the LLM needs a quick summary of the latest incidents or wants
    a starting point for investigation without reading the full dataset.
    Returns a list of incident dictionaries with chunk id, process details,
    description, row range, and timestamp when available.
    """
    return _get_recent_incidents_impl(n=n)


if __name__ == "__main__":
    print(ascii(_get_recent_incidents_impl()))
