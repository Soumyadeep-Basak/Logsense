"""Knowledge-base search helpers using local BM25 and embeddings."""

from __future__ import annotations

from typing import Any
try:
    from langchain_core.tools import tool
except ImportError:  # pragma: no cover - compatibility fallback
    from langchain.tools import tool

from app.tools._tool_data_loader import (
    bm25_tokenize,
    cosine_similarity,
    load_bm25_kb,
    load_kb_final,
    parse_embedding,
)

RRF_K = 60


def _search_kb_impl(
    query: str,
    process_type: str | None = None,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    """Return BM25-ranked KB results for a query with optional process-type filtering."""
    if not str(query).strip() or top_k <= 0:
        return []

    kb_df = load_kb_final()
    if kb_df.empty or "kb_chunk_id" not in kb_df.columns:
        return []

    corpus_df = _filter_corpus(kb_df, process_type)
    if corpus_df.empty:
        return []

    corpus_ids = [str(value) for value in corpus_df["kb_chunk_id"].astype(str).tolist()]
    bm25_rank_map = _build_bm25_rank_map(query=query, allowed_ids=set(corpus_ids))

    scored_rows: list[tuple[float, dict[str, Any]]] = []
    fallback_rank = len(corpus_ids) + 1
    for _, row in corpus_df.iterrows():
        kb_chunk_id = str(row.get("kb_chunk_id", ""))
        rank = bm25_rank_map.get(kb_chunk_id, fallback_rank)
        score = 1.0 / (RRF_K + rank)
        scored_rows.append((score, _row_to_kb_dict(row, score)))

    scored_rows.sort(key=lambda item: item[0], reverse=True)
    return [payload for _, payload in scored_rows[:top_k]]


def _search_kb_with_query_embedding_impl(
    query: str,
    query_embedding: list[float],
    process_type: str | None = None,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    """Return hybrid BM25 plus vector-ranked KB results for a query and embedding."""
    if not str(query).strip() or top_k <= 0:
        return []

    kb_df = load_kb_final()
    if kb_df.empty or "kb_chunk_id" not in kb_df.columns:
        return []

    corpus_df = _filter_corpus(kb_df, process_type)
    if corpus_df.empty:
        return []

    corpus_ids = [str(value) for value in corpus_df["kb_chunk_id"].astype(str).tolist()]
    allowed_ids = set(corpus_ids)
    bm25_rank_map = _build_bm25_rank_map(query=query, allowed_ids=allowed_ids)
    vector_rank_map = _build_vector_rank_map(corpus_df=corpus_df, query_embedding=query_embedding)

    scored_rows: list[tuple[float, dict[str, Any]]] = []
    fallback_rank = len(corpus_ids) + 1
    for _, row in corpus_df.iterrows():
        kb_chunk_id = str(row.get("kb_chunk_id", ""))
        bm25_rank = bm25_rank_map.get(kb_chunk_id, fallback_rank)
        vector_rank = vector_rank_map.get(kb_chunk_id, fallback_rank)
        score = (0.4 * (1.0 / (RRF_K + bm25_rank))) + (0.6 * (1.0 / (RRF_K + vector_rank)))
        scored_rows.append((score, _row_to_kb_dict(row, score)))

    scored_rows.sort(key=lambda item: item[0], reverse=True)
    return [payload for _, payload in scored_rows[:top_k]]


def _filter_corpus(kb_df, process_type: str | None):
    if not process_type or "kb_process_type" not in kb_df.columns:
        return kb_df

    filtered = kb_df[
        kb_df["kb_process_type"].astype(str).str.contains(str(process_type), case=False, na=False, regex=False)
    ]
    if len(filtered) < 3:
        return kb_df
    return filtered


def _build_bm25_rank_map(query: str, allowed_ids: set[str]) -> dict[str, int]:
    bm25_index, bm25_ids = load_bm25_kb()
    if bm25_index is None or not bm25_ids:
        return {}

    tokens = bm25_tokenize(query)
    if not tokens:
        return {}

    try:
        scores = bm25_index.get_scores(tokens)
    except Exception:  # noqa: BLE001
        return {}

    ranked: list[tuple[int, float]] = []
    for position, score in enumerate(scores):
        chunk_id = str(bm25_ids[position]) if position < len(bm25_ids) else ""
        if chunk_id not in allowed_ids:
            score = 0.0
        ranked.append((position, float(score)))

    ranked.sort(key=lambda item: item[1], reverse=True)
    return {
        str(bm25_ids[position]): rank + 1
        for rank, (position, _) in enumerate(ranked)
        if position < len(bm25_ids) and str(bm25_ids[position]) in allowed_ids
    }


def _build_vector_rank_map(corpus_df, query_embedding: list[float]) -> dict[str, int]:
    if not query_embedding:
        return {}

    similarities: list[tuple[str, float]] = []
    for _, row in corpus_df.iterrows():
        kb_chunk_id = str(row.get("kb_chunk_id", ""))
        doc_embedding = parse_embedding(str(row.get("kb_embedding", "")))
        if not doc_embedding:
            continue
        score = cosine_similarity(query_embedding, doc_embedding)
        similarities.append((kb_chunk_id, float(score)))

    similarities.sort(key=lambda item: item[1], reverse=True)
    return {kb_chunk_id: rank + 1 for rank, (kb_chunk_id, _) in enumerate(similarities)}


def _row_to_kb_dict(row, score: float) -> dict[str, Any]:
    return {
        "kb_chunk_id": str(row.get("kb_chunk_id", "")),
        "kb_title": str(row.get("kb_title", "")),
        "kb_url": str(row.get("kb_url", "")),
        "kb_text": str(row.get("kb_text", "")),
        "kb_process_type": str(row.get("kb_process_type", "")),
        "score": round(float(score), 4),
    }


@tool
def search_kb(
    query: str,
    process_type: str | None = None,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    """Search the local Ubuntu knowledge base for incident-relevant discussions.

    Use this when the LLM needs remediation context, prior issue threads, or
    supporting knowledge related to a log pattern, process, or failure mode.
    Returns a ranked list of KB result dictionaries with title, URL, text,
    process type, and score.
    """
    return _search_kb_impl(query=query, process_type=process_type, top_k=top_k)


@tool
def search_kb_with_query_embedding(
    query: str,
    query_embedding: list[float],
    process_type: str | None = None,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    """Run hybrid BM25 plus vector search over the local knowledge base.

    Use this when the LLM or caller already has a query embedding and wants
    stronger semantic matching than keyword search alone.
    Returns a ranked list of KB result dictionaries with fused hybrid scores.
    """
    return _search_kb_with_query_embedding_impl(
        query=query,
        query_embedding=query_embedding,
        process_type=process_type,
        top_k=top_k,
    )


if __name__ == "__main__":
    print(ascii(_search_kb_impl("ssh authentication failure")))
