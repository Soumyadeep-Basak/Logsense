"""Incident filtering helpers by process, time, and anomaly flag."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
try:
    from langchain_core.tools import tool
except ImportError:  # pragma: no cover - compatibility fallback
    from langchain.tools import tool

from app.tools._tool_data_loader import load_anomalies, load_final

LOGGER = logging.getLogger(__name__)


def _filter_incidents_impl(
    process_name: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    anomaly_only: bool = True,
) -> list[dict[str, Any]]:
    """Return incident summaries filtered by process, time range, and anomaly state."""
    df = load_final()
    if df.empty:
        return []

    filtered = df.copy()

    if anomaly_only:
        filtered = _apply_anomaly_filter(filtered)

    if process_name:
        if "process_name" not in filtered.columns:
            return []
        filtered = filtered[
            filtered["process_name"].astype(str).str.lower() == str(process_name).lower()
        ]

    if start_time or end_time:
        filtered = _apply_time_filter(filtered, start_time=start_time, end_time=end_time)

    if filtered.empty:
        return []

    sorted_df = _sort_incidents(filtered)
    return [_row_to_incident_dict(row) for _, row in sorted_df.iterrows()]


def _apply_anomaly_filter(df: pd.DataFrame) -> pd.DataFrame:
    anomalies = load_anomalies()
    if anomalies.empty or "is_anomaly" not in anomalies.columns or "chunk_id" not in anomalies.columns:
        return df

    truthy = anomalies["is_anomaly"].apply(_is_truthy)
    anomaly_ids = set(anomalies.loc[truthy, "chunk_id"].astype(str))
    if not anomaly_ids or "chunk_id" not in df.columns:
        return df.iloc[0:0] if anomaly_ids else df
    return df[df["chunk_id"].astype(str).isin(anomaly_ids)]


def _apply_time_filter(
    df: pd.DataFrame,
    start_time: str | None,
    end_time: str | None,
) -> pd.DataFrame:
    if "timestamp" not in df.columns:
        LOGGER.warning("Skipping time filter because timestamp column is missing.")
        return df

    timestamps = pd.to_datetime(df["timestamp"], errors="coerce")
    if timestamps.notna().sum() == 0:
        LOGGER.warning("Skipping time filter because timestamp parsing failed.")
        return df

    start_dt = pd.to_datetime(start_time, errors="coerce") if start_time else pd.NaT
    end_dt = pd.to_datetime(end_time, errors="coerce") if end_time else pd.NaT

    filtered = df.assign(_tool_timestamp=timestamps)
    filtered = filtered[filtered["_tool_timestamp"].notna()]
    if pd.notna(start_dt):
        filtered = filtered[filtered["_tool_timestamp"] >= start_dt]
    if pd.notna(end_dt):
        filtered = filtered[filtered["_tool_timestamp"] <= end_dt]
    return filtered


def _is_truthy(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _sort_incidents(df: pd.DataFrame) -> pd.DataFrame:
    if "_tool_timestamp" in df.columns and df["_tool_timestamp"].notna().any():
        return df.sort_values(by="_tool_timestamp", ascending=False, na_position="last")
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
def filter_incidents(
    process_name: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    anomaly_only: bool = True,
) -> list[dict[str, Any]]:
    """Filter incidents by process name, optional time range, and anomaly flag.

    Use this when the LLM needs a narrowed incident set, especially for
    anomaly-focused analysis or process-specific troubleshooting.
    Returns a list of incident summary dictionaries matching the requested filters.
    """
    return _filter_incidents_impl(
        process_name=process_name,
        start_time=start_time,
        end_time=end_time,
        anomaly_only=anomaly_only,
    )


if __name__ == "__main__":
    print(ascii(_filter_incidents_impl()))
