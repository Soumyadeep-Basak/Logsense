from __future__ import annotations

import json
from typing import Any

from app.agent.prompts import FINAL_PROMPT, REASON_PROMPT
from app.tools import (
    expand_incident_context,
    filter_incidents,
    get_incident_by_pid,
    get_process_profile,
    get_raw_log_window,
    get_recent_incidents,
    get_similar_incidents,
    search_kb,
    search_stackoverflow,
)

ALLOWED_ACTIONS = {
    "expand_context",
    "similar_incidents",
    "search_kb",
    "stackoverflow",
    "answer",
}
MAX_TOOL_STEPS = 3


def pre_retrieval_node(state: dict[str, Any]) -> dict[str, Any]:
    chunk_id = str(state.get("chunk_id", "")).strip()
    context = _normalize_tool_result(expand_incident_context.invoke({"chunk_id": chunk_id}))
    raw_log_lines = context.get("raw_log_lines", [])
    process_name = str(context.get("process_name", "")).strip()
    process_pid = str(context.get("process_pid", "")).strip()
    start_row = _safe_int(context.get("start_row", 0))
    end_row = _safe_int(context.get("end_row", 0))

    profile = _normalize_tool_result(
        get_process_profile.invoke({"process_name": process_name})
    ) if process_name else {}
    recent_incidents = _normalize_tool_results(get_recent_incidents.invoke({"n": 3}))
    pid_incidents = _normalize_tool_results(
        get_incident_by_pid.invoke({"pid": process_pid, "limit": 3})
    ) if process_pid else []
    filtered_process_incidents = _normalize_tool_results(
        filter_incidents.invoke({"process_name": process_name, "anomaly_only": True})
    )[:5] if process_name else []

    fallback_logs = _ensure_string_list(
        get_raw_log_window.invoke(
            {
                "start_row": start_row,
                "end_row": end_row,
                "process_name": process_name or None,
            }
        )
    ) if start_row or end_row else []

    combined_context = [context]
    if profile and not profile.get("error"):
        combined_context.append({"process_profile": profile})
    if recent_incidents:
        combined_context.append({"recent_incidents": recent_incidents})
    if pid_incidents:
        combined_context.append({"pid_incidents": pid_incidents})
    if filtered_process_incidents:
        combined_context.append({"filtered_process_incidents": filtered_process_incidents})

    return {
        "context": combined_context,
        "logs": _ensure_string_list(raw_log_lines) or fallback_logs,
        "steps": 0,
    }


def reasoning_node(state: dict[str, Any], llm) -> dict[str, Any]:
    steps = int(state.get("steps", 0))
    if steps >= MAX_TOOL_STEPS:
        return {"decision": "answer"}

    prompt = REASON_PROMPT.format(
        query=state.get("query", ""),
        context=json.dumps(state.get("context", []), ensure_ascii=True, default=str),
        risk_score=state.get("risk_score", 0.0),
        contributor_features=json.dumps(
            state.get("contributor_features", {}),
            ensure_ascii=True,
            default=str,
        ),
    )
    decision_payload = _invoke_json_llm(llm, prompt)
    action = str(decision_payload.get("action", "answer")).strip().lower()
    if action not in ALLOWED_ACTIONS:
        action = "answer"
    return {"decision": action}


def tool_node(state: dict[str, Any]) -> dict[str, Any]:
    action = str(state.get("decision", "")).strip().lower()
    chunk_id = str(state.get("chunk_id", "")).strip()
    context = state.get("context", []) or []
    primary_context = context[0] if context else {}
    process_name = str(primary_context.get("process_name", "")).strip()
    steps = int(state.get("steps", 0)) + 1

    if action == "similar_incidents":
        res = _normalize_tool_results(
            get_similar_incidents.invoke({"chunk_id": chunk_id, "top_k": 3, "min_score": 0.75})
        )
        return {"similar_incidents": res, "steps": steps}

    if action == "search_kb":
        res = _normalize_tool_results(
            search_kb.invoke(
                {
                    "query": _build_search_query(state),
                    "process_type": process_name or None,
                    "top_k": 5,
                }
            )
        )
        return {"kb_results": res, "steps": steps}

    if action == "expand_context":
        res = _normalize_tool_result(expand_incident_context.invoke({"chunk_id": chunk_id}))
        existing_context = list(context)
        if res and not res.get("error"):
            existing_context = [res] + existing_context[1:]
        return {
            "context": existing_context,
            "logs": _ensure_string_list(res.get("raw_log_lines", [])),
            "steps": steps,
        }

    if action == "stackoverflow":
        res = _normalize_tool_results(
            search_stackoverflow.invoke(
                {"query": _build_search_query(state), "top_k": 3}
            )
        )
        return {"kb_results": res, "steps": steps}

    return {"steps": steps}


def final_node(state: dict[str, Any], llm) -> dict[str, Any]:
    prompt = FINAL_PROMPT.format(
        query=state.get("query", ""),
        risk_score=state.get("risk_score", 0.0),
        contributor_features=json.dumps(
            state.get("contributor_features", {}),
            ensure_ascii=True,
            default=str,
        ),
        context=json.dumps(state.get("context", []), ensure_ascii=True, default=str),
        logs=json.dumps(state.get("logs", []), ensure_ascii=True, default=str),
        similar=json.dumps(state.get("similar_incidents", []), ensure_ascii=True, default=str),
        kb=json.dumps(state.get("kb_results", []), ensure_ascii=True, default=str),
    )

    answer = _invoke_json_llm(llm, prompt)
    answer["important_log_lines"] = _ensure_string_list(answer.get("important_log_lines", []))
    answer["troubleshooting"] = _ensure_string_list(answer.get("troubleshooting", []))
    answer["confidence"] = _safe_float(answer.get("confidence", 0.0))
    answer["risk_score"] = _clamp_score(answer.get("risk_score", state.get("risk_score", 0.0)))
    return {"answer": answer}


def _invoke_json_llm(llm, prompt: str) -> dict[str, Any]:
    response = llm.invoke(prompt)

    if isinstance(response, dict):
        return response

    content = getattr(response, "content", response)
    if isinstance(content, list):
        content = "".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in content
        )
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
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                return {}
        return {}


def _normalize_tool_result(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return result
    return {"value": result}


def _normalize_tool_results(result: Any) -> list[dict[str, Any]]:
    if isinstance(result, list):
        return [item if isinstance(item, dict) else {"value": item} for item in result]
    if isinstance(result, dict):
        return [result]
    return [{"value": result}]


def _build_search_query(state: dict[str, Any]) -> str:
    query = str(state.get("query", "")).strip()
    context = state.get("context", []) or []
    primary_context = context[0] if context else {}
    process_name = str(primary_context.get("process_name", "")).strip()
    high_level_description = str(primary_context.get("high_level_description", "")).strip()
    logs = _ensure_string_list(state.get("logs", []))
    sample_logs = " ".join(logs[:3]).strip()

    pieces = [query, process_name, high_level_description, sample_logs]
    return " ".join(piece for piece in pieces if piece).strip() or query


def _ensure_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clamp_score(value: Any) -> float:
    score = _safe_float(value)
    if score < 0.0:
        return 0.0
    if score > 1.0:
        return 1.0
    return score


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0
