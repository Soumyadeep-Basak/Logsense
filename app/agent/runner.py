from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from app.agent.graph import build_graph
from app.config import FINAL_PATH, GROQ_API_KEY_ENV_VAR, GROQ_INCIDENT_DESCRIPTION_MODEL

ANALYSIS_RESULTS_PATH = Path("app/data/processed/analysis_results.json")


class GroqJSONLLM:
    """Minimal Groq-backed adapter exposing an invoke(prompt) method."""

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

    def invoke(self, prompt: str) -> dict[str, Any]:
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
        content = response.choices[0].message.content or "{}"
        return _parse_json_response(content)


def run_pipeline(llm, limit: int | None = None) -> list[dict[str, Any]]:
    incidents = load_anomalies(limit=limit)
    graph = build_graph(llm)
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
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


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
