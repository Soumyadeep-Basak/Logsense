"""Incident filtering helpers for process ids."""

from __future__ import annotations

from typing import Any

import pandas as pd
try:
    from langchain_core.tools import tool
except ImportError:  # pragma: no cover - compatibility fallback
    from langchain.tools import tool

from app.tools._tool_data_loader import load_final


def _get_incident_by_pid_impl(pid: str, limit: int = 3) -> list[dict[str, Any]]:
    """Return recent incidents whose process_pid field contains the given pid."""
    if not str(pid).strip():
        return []

    df = load_final()
    if df.empty or "process_pid" not in df.columns:
        return []

    filtered = df[df["process_pid"].astype(str).str.contains(str(pid), na=False, regex=False)]
    if filtered.empty or limit <= 0:
        return []

    sorted_df = _sort_incidents(filtered)
    rows = sorted_df.head(min(limit, len(sorted_df)))
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
def get_incident_by_pid(pid: str, limit: int = 3) -> list[dict[str, Any]]:
    """Find incidents associated with a specific process PID string.

    Use this when the LLM already knows a PID from logs or prior context and
    wants matching incident chunks without scanning all incidents manually.
    Returns a list of incident summary dictionaries ordered by recency.
    """
    return _get_incident_by_pid_impl(pid=pid, limit=limit)


if __name__ == "__main__":
    print(ascii(_get_incident_by_pid_impl("123")))
