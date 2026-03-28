import json
import os
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from app.config import (
    ANOMALIES_PATH,
    CHUNKS_PATH,
    GROQ_API_KEY_ENV_VAR,
    GROQ_INCIDENT_DESCRIPTION_MODEL,
    GROQ_REQUEST_DELAY_SECONDS,
    GROQ_MAX_RETRIES,
)

MAX_ROW_TEXT_LENGTH = 180
OUTPUT_COLUMNS = [
    "chunk_id",
    "start_row",
    "end_row",
    "process_pid",
    "process_name",
    "description",
    "high_level_description",
    "low_level_description",
]
SYSTEM_PROMPT = """You are an incident triage assistant for system logs.

You will receive a chunk of related log rows from one potential incident.
Return a strict JSON object with exactly these keys:
- "high_level_description": one concise sentence for a human-friendly incident summary
- "low_level_description": one concise but technical sentence describing the concrete low-level behavior, signals, and likely scope

Rules:
- Base the answer only on the provided logs.
- Do not invent missing facts.
- Mention uncertainty when needed.
- Keep each value under 320 characters.
- Output JSON only. No markdown, no code fences, no extra text.
"""


def enrich_chunks_with_descriptions(
    chunks_csv: str | Path = CHUNKS_PATH,
    logs_csv: str | Path = ANOMALIES_PATH,
    output_csv: str | Path | None = None,
    model_name: str = GROQ_INCIDENT_DESCRIPTION_MODEL,
    request_delay_seconds: float = GROQ_REQUEST_DELAY_SECONDS,
    max_retries: int = GROQ_MAX_RETRIES,
) -> None:
    """Populate chunk descriptions using Groq and write the enriched CSV."""
    _load_env()
    api_key = os.getenv(GROQ_API_KEY_ENV_VAR)
    if not api_key:
        raise RuntimeError(f"Missing {GROQ_API_KEY_ENV_VAR} in environment.")

    try:
        from groq import Groq
    except ImportError as error:
        raise RuntimeError(
            "Missing dependency 'groq'. Install it with: .\\.venv\\Scripts\\python.exe -m pip install groq"
        ) from error

    client = Groq(api_key=api_key)
    chunks = pd.read_csv(chunks_csv)
    logs = pd.read_csv(logs_csv)

    index_column = _resolve_index_column(logs)
    chunks = _ensure_output_columns(chunks)
    logs = _normalize_logs(logs, index_column)

    for row_number, chunk in enumerate(chunks.itertuples(index=False), start=1):
        chunk_logs = _slice_chunk_logs(logs, int(chunk.start_row), int(chunk.end_row))
        prompt = _build_user_prompt(chunk, chunk_logs, compact_mode="full")
        descriptions = _request_descriptions(
            client=client,
            prompt=prompt,
            model_name=model_name,
            max_retries=max_retries,
            chunk=chunk,
            chunk_logs=chunk_logs,
        )

        chunks.at[row_number - 1, "high_level_description"] = descriptions[
            "high_level_description"
        ]
        chunks.at[row_number - 1, "low_level_description"] = descriptions[
            "low_level_description"
        ]

        if row_number < len(chunks):
            time.sleep(request_delay_seconds)

    destination = Path(output_csv or chunks_csv)
    destination.parent.mkdir(parents=True, exist_ok=True)
    chunks.to_csv(destination, index=False)


def _load_env() -> None:
    app_env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(app_env_path)
    load_dotenv()


def _resolve_index_column(logs: pd.DataFrame) -> str:
    for candidate in ("row_id", "row_no"):
        if candidate in logs.columns:
            return candidate
    raise KeyError("Missing required row index column. Expected one of: row_id, row_no")


def _ensure_output_columns(chunks: pd.DataFrame) -> pd.DataFrame:
    enriched = chunks.copy()
    if "description" not in enriched.columns:
        enriched["description"] = ""
    else:
        enriched["description"] = enriched["description"].fillna("")
    for column in ("high_level_description", "low_level_description"):
        if column not in enriched.columns:
            enriched[column] = ""
        else:
            enriched[column] = enriched[column].fillna("")

    enriched = enriched.reindex(columns=OUTPUT_COLUMNS)
    enriched["process_pid"] = enriched["process_pid"].apply(_serialize_pid)
    return enriched


def _normalize_logs(logs: pd.DataFrame, index_column: str) -> pd.DataFrame:
    normalized = logs.copy()
    normalized[index_column] = pd.to_numeric(normalized[index_column], errors="coerce")
    normalized = normalized.dropna(subset=[index_column]).copy()
    normalized[index_column] = normalized[index_column].astype(int)
    normalized["process_pid"] = pd.to_numeric(_get_series(normalized, "process_pid", ""), errors="coerce")
    normalized["process_name"] = _get_series(normalized, "process_name", "").fillna("").astype(str)
    normalized["parsed_logline"] = _get_series(normalized, "parsed_logline", "").fillna("").astype(str)
    normalized["logline"] = _get_series(normalized, "logline", "").fillna("").astype(str)
    normalized["is_anomaly"] = _get_series(normalized, "is_anomaly", False).apply(_to_bool)
    normalized["row_index"] = normalized[index_column]
    return normalized.sort_values("row_index").reset_index(drop=True)


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


def _slice_chunk_logs(logs: pd.DataFrame, start_row: int, end_row: int) -> pd.DataFrame:
    return logs.loc[(logs["row_index"] >= start_row) & (logs["row_index"] <= end_row)].copy()


def _build_user_prompt(chunk, chunk_logs: pd.DataFrame, compact_mode: str) -> str:
    rendered_rows = [_render_log_row(row, compact_mode) for row in chunk_logs.itertuples(index=False)]

    chunk_metadata = {
        "chunk_id": chunk.chunk_id,
        "start_row": int(chunk.start_row),
        "end_row": int(chunk.end_row),
        "center_process_name": chunk.process_name,
        "center_process_pid": _serialize_pid(chunk.process_pid),
        "prompt_mode": compact_mode,
    }

    return (
        "Summarize this incident chunk.\n"
        f"Chunk metadata: {json.dumps(chunk_metadata, ensure_ascii=True)}\n"
        "Log rows:\n"
        + "\n".join(rendered_rows)
    )


def _render_log_row(row, compact_mode: str) -> str:
    row_id = int(row.row_index)
    process_name = row.process_name
    process_pid = _serialize_pid(row.process_pid)
    anomaly_flag = "1" if bool(row.is_anomaly) else "0"
    raw_log = _compact_text(row.logline, limit=MAX_ROW_TEXT_LENGTH)
    parsed_log = _compact_text(row.parsed_logline, limit=MAX_ROW_TEXT_LENGTH)

    if compact_mode == "full":
        return (
            f"row={row_id} anomaly={anomaly_flag} process={process_name} pid={process_pid} "
            f'parsed="{parsed_log}" raw="{raw_log}"'
        )

    if compact_mode == "parsed_only":
        return (
            f"row={row_id} anomaly={anomaly_flag} process={process_name} pid={process_pid} "
            f'parsed="{parsed_log}"'
        )

    return (
        f"row={row_id} anomaly={anomaly_flag} process={process_name} pid={process_pid} "
        f'event="{_compact_text(parsed_log or raw_log, limit=90)}"'
    )


def _compact_text(value, limit: int) -> str:
    text = str(value or "").replace("\n", " ").replace("\r", " ")
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _serialize_pid(pid_value):
    if pd.isna(pid_value):
        return ""
    if isinstance(pid_value, str):
        return pid_value
    if float(pid_value).is_integer():
        return int(pid_value)
    return pid_value


def _request_descriptions(
    client,
    prompt: str,
    model_name: str,
    max_retries: int,
    chunk,
    chunk_logs: pd.DataFrame,
) -> dict[str, str]:
    last_error = None
    prompt_modes = ["full", "parsed_only", "minimal"]

    for prompt_mode in prompt_modes:
        active_prompt = prompt if prompt_mode == "full" else _build_user_prompt(
            chunk,
            chunk_logs,
            compact_mode=prompt_mode,
        )

        for attempt in range(1, max_retries + 1):
            try:
                response = client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": active_prompt},
                    ],
                    temperature=0.2,
                    max_completion_tokens=400,
                    top_p=1,
                    stream=False,
                )
                content = response.choices[0].message.content or "{}"
                return _parse_descriptions(content)
            except Exception as error:  # noqa: BLE001
                last_error = error
                error_text = str(error)
                if "Request too large" in error_text or "rate_limit_exceeded" in error_text:
                    break
                if attempt == max_retries:
                    break
                time.sleep(attempt * 5)

    raise RuntimeError(f"Failed to enrich chunk descriptions: {last_error}") from last_error


def _parse_descriptions(content: str) -> dict[str, str]:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.replace("json", "", 1).strip()

    payload = json.loads(cleaned)
    return {
        "high_level_description": str(payload.get("high_level_description", "")).strip(),
        "low_level_description": str(payload.get("low_level_description", "")).strip(),
    }


if __name__ == "__main__":
    enrich_chunks_with_descriptions()
