from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from app.config import (
    COHERE_API_KEY_ENV_VAR,
    COHERE_EMBED_MODEL,
    GROQ_API_BASE_URL,
    GROQ_API_KEY_ENV_VAR,
    GROQ_INCIDENT_DESCRIPTION_MODEL,
)

RAGAS_SCORES_PATH = Path("app/data/processed/ragas_scores.json")


def load_results(json_path: str):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def prepare_ragas_dataset(results):
    from datasets import Dataset

    records = []

    for item in results:
        question = "Analyze anomaly in logs"

        contexts = []
        contexts.extend(_ensure_string_list(item.get("logs", [])))
        contexts.extend(_ensure_string_list(item.get("logs_used", [])))

        kb_entries = item.get("kb", []) or item.get("kb_used", [])
        for kb in kb_entries:
            if isinstance(kb, dict):
                contexts.append(str(kb.get("kb_text", "")))
            else:
                contexts.append(str(kb))

        contexts = [context for context in contexts if str(context).strip()]
        answer_payload = item.get("answer", {}) if isinstance(item.get("answer", {}), dict) else {}
        answer = str(answer_payload.get("issue", "")).strip()

        records.append(
            {
                "question": question,
                "contexts": contexts,
                "answer": answer,
                "ground_truth": answer,
            }
        )

    return Dataset.from_pandas(pd.DataFrame(records))


def run_ragas_eval(json_path: str):
    from ragas import evaluate
    from ragas.metrics import AnswerRelevancy, context_precision, context_recall, faithfulness

    _load_env()
    results = load_results(json_path)
    if not results:
        save_scores({}, path=str(RAGAS_SCORES_PATH))
        return {}

    dataset = prepare_ragas_dataset(results)
    evaluator_llm = _build_ragas_llm()
    evaluator_embeddings = _build_ragas_embeddings()
    answer_relevancy_metric = AnswerRelevancy(strictness=1)

    scores = evaluate(
        dataset,
        metrics=[
            context_precision,
            context_recall,
            faithfulness,
            answer_relevancy_metric,
        ],
        llm=evaluator_llm,
        embeddings=evaluator_embeddings,
    )
    serializable_scores = _scores_to_dict(scores)
    print(serializable_scores)
    save_scores(serializable_scores)
    return serializable_scores


def save_scores(scores, path="app/data/processed/ragas_scores.json"):
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(scores, f, indent=2)


def _scores_to_dict(scores) -> dict:
    if hasattr(scores, "to_pandas"):
        score_df = scores.to_pandas()
        if not score_df.empty:
            return _score_dataframe_to_payload(score_df)
        return {}
    if hasattr(scores, "to_dict"):
        return _clean_score_mapping(scores.to_dict())
    return _clean_score_mapping(dict(scores))


def _ensure_string_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def _build_ragas_llm():
    groq_api_key = os.getenv(GROQ_API_KEY_ENV_VAR, "").strip()
    if not groq_api_key:
        raise RuntimeError(
            f"Missing {GROQ_API_KEY_ENV_VAR} for Ragas evaluation."
        )

    try:
        from langchain_openai import ChatOpenAI
    except ImportError as error:
        raise RuntimeError(
            "Missing dependency 'langchain-openai'. Install it with: "
            ".\\.venv\\Scripts\\python.exe -m pip install langchain-openai"
        ) from error

    return ChatOpenAI(
        model=GROQ_INCIDENT_DESCRIPTION_MODEL,
        api_key=groq_api_key,
        base_url=GROQ_API_BASE_URL,
        temperature=0.0,
    )


def _build_ragas_embeddings():
    cohere_api_key = os.getenv(COHERE_API_KEY_ENV_VAR, "").strip()
    if not cohere_api_key:
        raise RuntimeError(
            f"Missing {COHERE_API_KEY_ENV_VAR} for Ragas evaluation."
        )

    try:
        from langchain_cohere import CohereEmbeddings
    except ImportError as error:
        raise RuntimeError(
            "Missing dependency 'langchain-cohere'. Install it with: "
            ".\\.venv\\Scripts\\python.exe -m pip install langchain-cohere"
        ) from error

    return CohereEmbeddings(
        model=COHERE_EMBED_MODEL,
        cohere_api_key=cohere_api_key,
    )


def _load_env() -> None:
    app_env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(app_env_path)
    load_dotenv()


def _clean_score_mapping(values: dict) -> dict:
    cleaned = {}
    for key, value in values.items():
        cleaned[key] = _normalize_score_value(value)
    return cleaned


def _score_dataframe_to_payload(score_df: pd.DataFrame) -> dict:
    metric_columns = [
        "context_precision",
        "context_recall",
        "faithfulness",
        "answer_relevancy",
    ]
    aggregate: dict[str, float | None] = {}
    for column in metric_columns:
        if column not in score_df.columns:
            continue
        numeric_series = pd.to_numeric(score_df[column], errors="coerce")
        non_null = numeric_series.dropna()
        aggregate[column] = float(non_null.mean()) if not non_null.empty else None

    by_sample = [
        _clean_score_mapping(record)
        for record in score_df.to_dict(orient="records")
    ]
    return {
        "aggregate": aggregate,
        "by_sample": by_sample,
    }


def _normalize_score_value(value):
    if isinstance(value, pd.Series):
        return [_normalize_score_value(item) for item in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [_normalize_score_value(item) for item in value]
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, dict)):
        try:
            converted = value.tolist()
        except Exception:  # noqa: BLE001
            converted = value
        if isinstance(converted, list):
            return [_normalize_score_value(item) for item in converted]
        value = converted
    if isinstance(value, dict):
        return {key: _normalize_score_value(item) for key, item in value.items()}
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value
