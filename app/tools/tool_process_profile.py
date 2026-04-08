"""Helpers for building a process-level incident profile."""

from __future__ import annotations

from typing import Any
try:
    from langchain_core.tools import tool
except ImportError:  # pragma: no cover - compatibility fallback
    from langchain.tools import tool

from app.tools._tool_data_loader import load_anomalies, load_final, load_parsed_logs


def _get_process_profile_impl(process_name: str) -> dict[str, Any]:
    """Return incident counts, anomaly rate, templates, and known PIDs for a process."""
    df = load_final()
    if df.empty or "process_name" not in df.columns:
        return {"error": f"No incidents found for process {process_name}"}

    proc_df = df[df["process_name"].astype(str).str.lower() == str(process_name).lower()]
    if proc_df.empty:
        return {"error": f"No incidents found for process {process_name}"}

    total_incidents = len(proc_df)
    anomaly_count = _count_anomalies(proc_df)
    anomaly_rate = round(anomaly_count / total_incidents, 4) if total_incidents else 0.0
    common_templates = _get_common_templates(process_name)
    pid_list = _get_known_pids(proc_df)

    return {
        "process_name": str(process_name),
        "total_incidents": total_incidents,
        "anomaly_count": int(anomaly_count),
        "anomaly_rate": round(float(anomaly_rate), 4),
        "common_templates": common_templates,
        "known_pids": pid_list,
    }


def _count_anomalies(proc_df) -> int:
    anomalies = load_anomalies()
    if anomalies.empty or "is_anomaly" not in anomalies.columns or "chunk_id" not in anomalies.columns:
        return 0

    anomaly_ids = set(
        anomalies.loc[anomalies["is_anomaly"].apply(_is_truthy), "chunk_id"].astype(str)
    )
    return int(proc_df["chunk_id"].astype(str).isin(anomaly_ids).sum()) if "chunk_id" in proc_df.columns else 0


def _get_common_templates(process_name: str) -> list[str]:
    parsed = load_parsed_logs()
    if parsed.empty or "template" not in parsed.columns:
        return []

    process_column = "process" if "process" in parsed.columns else "process_name" if "process_name" in parsed.columns else None
    if process_column is None:
        return []

    all_rows = parsed[parsed[process_column].astype(str).str.lower() == str(process_name).lower()]
    if all_rows.empty:
        return []

    return [str(value) for value in all_rows["template"].value_counts().head(5).index.tolist()]


def _get_known_pids(proc_df) -> list[str]:
    all_pids: set[str] = set()
    if "process_pid" not in proc_df.columns:
        return []

    for value in proc_df["process_pid"].dropna().astype(str):
        for pid in value.split(","):
            cleaned = pid.strip()
            if cleaned:
                all_pids.add(cleaned)
    return sorted(all_pids)[:10]


def _is_truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


@tool
def get_process_profile(process_name: str) -> dict[str, Any]:
    """Summarize how a process appears across incidents and parsed logs.

    Use this when the LLM needs a quick profile for one process, including
    incident volume, anomaly rate, common templates, and known PIDs.
    Returns one dictionary describing the process profile or an error payload
    if that process is not found in the incident data.
    """
    return _get_process_profile_impl(process_name=process_name)


if __name__ == "__main__":
    print(ascii(_get_process_profile_impl("sshd")))
