from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
from dotenv import load_dotenv

from app.agent.graph import build_graph
from app.config import (
    FINAL_PATH,
    GROQ_API_KEY_ENV_VAR,
    GROQ_INCIDENT_DESCRIPTION_MODEL,
    GROQ_MAX_RETRIES,
    GROQ_REQUEST_DELAY_SECONDS,
)
from app.eval.langsmith_setup import setup_langsmith
from app.eval.ragas_eval import run_ragas_eval

try:
    from langchain_core.outputs import Generation, LLMResult
except ImportError:  # pragma: no cover - optional runtime dependency path
    Generation = None
    LLMResult = None

ANALYSIS_RESULTS_PATH = Path("app/data/processed/analysis_results.json")
LLM_PARSE_FAILURE_PATH = Path("app/data/processed/last_llm_parse_failure.txt")


class GroqJSONLLM:
    """Minimal Groq-backed adapter exposing an invoke(prompt) method."""
    _last_request_time = 0.0

    def __init__(self, model_name: str = GROQ_INCIDENT_DESCRIPTION_MODEL) -> None:
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

        self.model_name = model_name
        self._client = Groq(api_key=api_key)
        self.request_delay_seconds = float(GROQ_REQUEST_DELAY_SECONDS)
        self.max_retries = int(GROQ_MAX_RETRIES)

    def invoke(self, prompt: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
        callbacks = list((config or {}).get("callbacks", []) or [])
        run_ids = self._start_llm_callbacks(callbacks, prompt)

        try:
            response = self._create_chat_completion_with_retries(prompt)
            content = response.choices[0].message.content or "{}"
            self._end_llm_callbacks(callbacks, run_ids, content)
            return _parse_json_response(content)
        except Exception as error:  # noqa: BLE001
            self._error_llm_callbacks(callbacks, run_ids, error)
            raise

    def _create_chat_completion_with_retries(self, prompt: str):
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._wait_for_request_slot()
            try:
                response = self._client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {
                            "role": "system",
                            "content": "Return valid JSON only. No markdown fences or extra text.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.2,
                    max_completion_tokens=1200,
                    top_p=1,
                    stream=False,
                )
                self._mark_request_time()
                return response
            except Exception as error:  # noqa: BLE001
                last_error = error
                if not _is_rate_limit_error(error) or attempt >= self.max_retries:
                    raise
                wait_seconds = _extract_retry_after_seconds(error, attempt=attempt)
                print(
                    f"Groq rate limit hit for {self.model_name}. "
                    f"Waiting {wait_seconds:.2f}s before retry {attempt + 1}/{self.max_retries}."
                )
                time.sleep(wait_seconds)
                self._mark_request_time()
        if last_error is not None:
            raise last_error
        raise RuntimeError("Groq request failed without an exception.")

    def _wait_for_request_slot(self) -> None:
        elapsed = time.monotonic() - self.__class__._last_request_time
        wait_seconds = self.request_delay_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _mark_request_time(self) -> None:
        self.__class__._last_request_time = time.monotonic()

    def _start_llm_callbacks(self, callbacks: list[Any], prompt: str) -> list[tuple[Any, Any]]:
        runs: list[tuple[Any, Any]] = []
        for callback in callbacks:
            if not hasattr(callback, "on_llm_start"):
                continue
            run_id = uuid4()
            try:
                callback.on_llm_start(
                    {"name": "GroqJSONLLM", "model": self.model_name},
                    [prompt],
                    run_id=run_id,
                    name="GroqJSONLLM",
                    metadata={"model_name": self.model_name},
                )
                runs.append((callback, run_id))
            except Exception:  # noqa: BLE001
                continue
        return runs

    def _end_llm_callbacks(self, callbacks: list[Any], run_ids: list[tuple[Any, Any]], content: str) -> None:
        if not (LLMResult and Generation):
            return

        callback_run_ids = {id(callback): run_id for callback, run_id in run_ids}
        response = LLMResult(generations=[[Generation(text=str(content))]])
        for callback in callbacks:
            run_id = callback_run_ids.get(id(callback))
            if run_id is None or not hasattr(callback, "on_llm_end"):
                continue
            try:
                callback.on_llm_end(response, run_id=run_id)
            except Exception:  # noqa: BLE001
                continue

    def _error_llm_callbacks(
        self,
        callbacks: list[Any],
        run_ids: list[tuple[Any, Any]],
        error: Exception,
    ) -> None:
        callback_run_ids = {id(callback): run_id for callback, run_id in run_ids}
        for callback in callbacks:
            run_id = callback_run_ids.get(id(callback))
            if run_id is None or not hasattr(callback, "on_llm_error"):
                continue
            try:
                callback.on_llm_error(error, run_id=run_id)
            except Exception:  # noqa: BLE001
                continue


def run_pipeline(llm, limit: int | None = None) -> list[dict[str, Any]]:
    incidents = load_anomalies(limit=limit)
    tracer = setup_langsmith()
    graph = build_graph(llm, tracer=tracer)
    results: list[dict[str, Any]] = []

    for row in incidents:
        chunk_id = str(row.get("chunk_id", "")).strip()
        if not chunk_id:
            continue

        state = {
            "query": _build_query(row),
            "chunk_id": chunk_id,
            "risk_score": _safe_float(row.get("risk_score", 0.0)),
            "contributor_features": _extract_contributor_features(row),
            "context": [],
            "kb_results": [],
            "similar_incidents": [],
            "logs": [],
            "decision": "",
            "steps": 0,
            "answer": {},
        }

        output = graph.invoke(state)
        final_answer = output.get("answer", {})
        generated_risk_score = _safe_float(final_answer.get("risk_score", state["risk_score"]))
        result = {
            "chunk_id": chunk_id,
            "risk_score": generated_risk_score,
            "contributor_features": state["contributor_features"],
            "logs_used": output.get("logs", []),
            "kb_used": output.get("kb_results", []),
            "similar_incidents_used": output.get("similar_incidents", []),
            "answer": final_answer,
        }
        results.append(result)

    ANALYSIS_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    ANALYSIS_RESULTS_PATH.write_text(json.dumps(results, indent=2), encoding="utf-8")
    try:
        run_ragas_eval(str(ANALYSIS_RESULTS_PATH))
    except Exception as error:  # noqa: BLE001
        print(f"Warning: Ragas evaluation failed: {error}")
    return results


def run_pipeline_with_groq(
    model_name: str = GROQ_INCIDENT_DESCRIPTION_MODEL,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    llm = GroqJSONLLM(model_name=model_name)
    return run_pipeline(llm=llm, limit=limit)


def load_anomalies(limit: int | None = None) -> list[dict[str, Any]]:
    if not FINAL_PATH.exists():
        return []

    df = pd.read_csv(FINAL_PATH)
    if df.empty or "chunk_id" not in df.columns:
        return []

    if "anomaly_flag" in df.columns:
        df = df[df["anomaly_flag"].fillna(0).astype(int) == 1]

    records = df.to_dict(orient="records")
    for record in records:
        record["risk_score"] = _safe_float(record.get("risk_score", 0.0))
    return records[:limit] if limit is not None and limit > 0 else records


def _build_query(row: dict[str, Any]) -> str:
    process_name = str(row.get("process_name", "")).strip()
    description = str(row.get("description", "")).strip()
    dominant_pid = str(row.get("dominant_pid", "")).strip()

    parts = ["Analyze this anomaly"]
    if process_name:
        parts.append(f"process={process_name}")
    if dominant_pid:
        parts.append(f"dominant_pid={dominant_pid}")
    if description:
        parts.append(description)
    return " | ".join(parts)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _extract_contributor_features(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "anomaly_count": _safe_int(row.get("anomaly_count", 0)),
        "pid_count": _safe_int(row.get("pid_count", 0)),
        "dominant_pid": str(row.get("dominant_pid", "")).strip(),
        "process_frequency": _safe_int(row.get("process_frequency", 0)),
        "is_critical_process": _safe_int(row.get("is_critical_process", 0)),
        "recency_score": _safe_float(row.get("recency_score", 0.0)),
        "log_density": _safe_float(row.get("log_density", 0.0)),
        "template_diversity": _safe_int(row.get("template_diversity", 0)),
    }


def _load_env() -> None:
    app_env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(app_env_path)
    load_dotenv()


def _parse_json_response(content: str) -> dict[str, Any]:
    text = str(content).strip()
    if not text:
        return {}

    if text.startswith("```"):
        stripped = text.strip("`")
        if stripped.lower().startswith("json"):
            stripped = stripped[4:].strip()
        text = stripped

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        for candidate in (
            text,
            _extract_json_code_block(text),
            _extract_first_balanced_json_snippet(text),
        ):
            if not candidate:
                continue
            parsed = _extract_first_json_object(candidate)
            if parsed is not None:
                return parsed

        _save_llm_parse_failure(text)
        return {}


def _extract_first_json_object(text: str) -> dict[str, Any] | None:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char not in "{[":
            continue
        try:
            value, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return None


def _extract_json_code_block(text: str) -> str:
    marker = "```json"
    lower_text = text.lower()
    start = lower_text.find(marker)
    if start == -1:
        return ""
    start += len(marker)
    end = text.find("```", start)
    if end == -1:
        return text[start:].strip()
    return text[start:end].strip()


def _extract_first_balanced_json_snippet(text: str) -> str:
    start = text.find("{")
    if start == -1:
        return ""

    depth = 0
    in_string = False
    escaped = False
    for index, char in enumerate(text[start:], start=start):
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return ""


def _save_llm_parse_failure(text: str) -> None:
    try:
        LLM_PARSE_FAILURE_PATH.parent.mkdir(parents=True, exist_ok=True)
        LLM_PARSE_FAILURE_PATH.write_text(text, encoding="utf-8")
    except OSError:
        pass


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _is_rate_limit_error(error: Exception) -> bool:
    error_name = type(error).__name__.lower()
    error_text = str(error).lower()
    return "ratelimit" in error_name or "rate limit" in error_text or "429" in error_text


def _extract_retry_after_seconds(error: Exception, attempt: int = 0) -> float:
    match = re.search(r"try again in ([0-9]+(?:\.[0-9]+)?)s", str(error), flags=re.IGNORECASE)
    base_wait = max(GROQ_REQUEST_DELAY_SECONDS, 2.0)
    if match:
        try:
            hinted_wait = float(match.group(1)) + 0.5
            return max(hinted_wait, base_wait * (attempt + 1))
        except ValueError:
            pass
    return base_wait * (attempt + 2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Logsense agentic RAG pipeline.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional number of anomalous chunks to process.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=GROQ_INCIDENT_DESCRIPTION_MODEL,
        help="Groq model name to use for reasoning and final answer generation.",
    )
    args = parser.parse_args()

    results = run_pipeline_with_groq(model_name=args.model, limit=args.limit)
    print(f"Processed {len(results)} anomalous chunk(s).")
    print(f"Saved analysis results to: {ANALYSIS_RESULTS_PATH}")


if __name__ == "__main__":
    main()
