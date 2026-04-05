"""Cached local data loaders and helpers for Logsense tools."""

from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from rank_bm25 import BM25Okapi
except ImportError:  # pragma: no cover - optional local dependency
    BM25Okapi = Any  # type: ignore[assignment]

LOGGER = logging.getLogger(__name__)

FINAL_CSV = Path("app/data/processed/final.csv")
PARSED_LOGS_CSV = Path("app/data/processed/parsed_logs.csv")
ANOMALIES_CSV = Path("app/data/processed/anomalies.csv")
KB_FINAL_CSV = Path("app/data/kb_processed/kb_final.csv")
BM25_KB_PKL = Path("app/data/retrieval/bm25_kb.pkl")
BM25_KB_IDS = Path("app/data/retrieval/bm25_kb_ids.json")
BM25_LOGS_PKL = Path("app/data/retrieval/bm25_logs.pkl")
BM25_LOG_IDS = Path("app/data/retrieval/bm25_log_ids.json")

_cache_final: pd.DataFrame | None = None
_cache_parsed_logs: pd.DataFrame | None = None
_cache_anomalies: pd.DataFrame | None = None
_cache_kb_final: pd.DataFrame | None = None
_cache_bm25_kb: BM25Okapi | None = None
_cache_bm25_kb_ids: list[str] | None = None
_cache_bm25_logs: BM25Okapi | None = None
_cache_bm25_log_ids: list[str] | None = None


def load_final() -> pd.DataFrame:
    """Return cached final incident data as a DataFrame."""
    global _cache_final
    if _cache_final is None:
        _cache_final = _read_csv(FINAL_CSV)
    return _cache_final


def load_parsed_logs() -> pd.DataFrame:
    """Return cached parsed log rows as a DataFrame."""
    global _cache_parsed_logs
    if _cache_parsed_logs is None:
        _cache_parsed_logs = _read_csv(PARSED_LOGS_CSV)
    return _cache_parsed_logs


def load_anomalies() -> pd.DataFrame:
    """Return cached anomaly labels as a DataFrame."""
    global _cache_anomalies
    if _cache_anomalies is None:
        _cache_anomalies = _read_csv(ANOMALIES_CSV)
    return _cache_anomalies


def load_kb_final() -> pd.DataFrame:
    """Return cached knowledge-base chunks as a DataFrame."""
    global _cache_kb_final
    if _cache_kb_final is None:
        _cache_kb_final = _read_csv(KB_FINAL_CSV)
    return _cache_kb_final


def load_bm25_kb() -> tuple[BM25Okapi | None, list[str]]:
    """Return cached KB BM25 index and ordered chunk ids."""
    global _cache_bm25_kb, _cache_bm25_kb_ids
    if _cache_bm25_kb is None:
        _cache_bm25_kb = _read_pickle(BM25_KB_PKL)
    if _cache_bm25_kb_ids is None:
        _cache_bm25_kb_ids = _read_json_list(BM25_KB_IDS)
    return _cache_bm25_kb, _cache_bm25_kb_ids or []


def load_bm25_logs() -> tuple[BM25Okapi | None, list[str]]:
    """Return cached log BM25 index and ordered chunk ids."""
    global _cache_bm25_logs, _cache_bm25_log_ids
    if _cache_bm25_logs is None:
        _cache_bm25_logs = _read_pickle(BM25_LOGS_PKL)
    if _cache_bm25_log_ids is None:
        _cache_bm25_log_ids = _read_json_list(BM25_LOG_IDS)
    return _cache_bm25_logs, _cache_bm25_log_ids or []


def parse_embedding(embedding_str: str) -> list[float]:
    """Parse a JSON embedding string into a list of floats."""
    try:
        parsed = json.loads(embedding_str)
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(parsed, list):
        return []
    values: list[float] = []
    for item in parsed:
        try:
            values.append(float(item))
        except (TypeError, ValueError):
            return []
    return values


def cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    """Compute cosine similarity between two numeric vectors."""
    import math

    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    mag_a = math.sqrt(sum(a * a for a in vec_a))
    mag_b = math.sqrt(sum(b * b for b in vec_b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def bm25_tokenize(text: str) -> list[str]:
    """Tokenize text for BM25 lookup."""
    import re

    return re.findall(r"[a-z0-9_/.-]+", str(text).lower())


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        LOGGER.warning("CSV not found: %s", path)
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as error:  # noqa: BLE001
        LOGGER.warning("Failed to read CSV %s: %s", path, error)
        return pd.DataFrame()


def _read_pickle(path: Path) -> Any:
    if not path.exists():
        LOGGER.warning("Pickle not found: %s", path)
        return None
    try:
        with path.open("rb") as handle:
            return pickle.load(handle)
    except Exception as error:  # noqa: BLE001
        LOGGER.warning("Failed to read pickle %s: %s", path, error)
        return None


def _read_json_list(path: Path) -> list[str]:
    if not path.exists():
        LOGGER.warning("JSON not found: %s", path)
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as error:  # noqa: BLE001
        LOGGER.warning("Failed to read JSON %s: %s", path, error)
        return []
    if not isinstance(data, list):
        return []
    return [str(item) for item in data]
