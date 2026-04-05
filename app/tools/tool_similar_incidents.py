"""Helpers for finding semantically similar incidents."""

from __future__ import annotations

from typing import Any

from app.tools._tool_data_loader import cosine_similarity, load_final, parse_embedding


def get_similar_incidents(
    chunk_id: str,
    top_k: int = 3,
    min_score: float = 0.75,
) -> list[dict[str, Any]]:
    """Return incidents similar to the given chunk id using stored embeddings."""
    df = load_final()
    if df.empty or "chunk_id" not in df.columns or top_k <= 0:
        return []

    source_rows = df[df["chunk_id"].astype(str) == str(chunk_id)]
    if source_rows.empty:
        return []

    source_embedding = parse_embedding(str(source_rows.iloc[0].get("embedding", "")))
    if not source_embedding:
        return [{"error": "no embedding found for chunk_id"}]

    parsed_embeddings: list[tuple[str, dict[str, Any], list[float]]] = []
    for _, row in df.iterrows():
        other_chunk_id = str(row.get("chunk_id", ""))
        if other_chunk_id == str(chunk_id):
            continue
        embedding = parse_embedding(str(row.get("embedding", "")))
        if not embedding:
            continue
        parsed_embeddings.append(
            (
                other_chunk_id,
                {
                    "process_name": str(row.get("process_name", "")),
                    "process_pid": str(row.get("process_pid", "")),
                    "description": str(row.get("description", "")),
                    "timestamp": str(row.get("timestamp", "")) if "timestamp" in row.index else "",
                },
                embedding,
            )
        )

    matches: list[tuple[float, dict[str, Any]]] = []
    for other_chunk_id, payload, embedding in parsed_embeddings:
        similarity = cosine_similarity(source_embedding, embedding)
        if similarity < min_score:
            continue
        matches.append(
            (
                similarity,
                {
                    "chunk_id": other_chunk_id,
                    "process_name": payload["process_name"],
                    "process_pid": payload["process_pid"],
                    "description": payload["description"],
                    "similarity_score": round(float(similarity), 4),
                    "timestamp": payload["timestamp"],
                },
            )
        )

    matches.sort(key=lambda item: item[0], reverse=True)
    return [payload for _, payload in matches[:top_k]]


if __name__ == "__main__":
    print(ascii(get_similar_incidents("chunk_0")))
