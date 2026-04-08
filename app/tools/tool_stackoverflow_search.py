"""Lightweight StackOverflow search helper."""

from __future__ import annotations

import html
import re
from typing import Any
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
try:
    from langchain_core.tools import tool
except ImportError:  # pragma: no cover - compatibility fallback
    from langchain.tools import tool

DUCKDUCKGO_HTML_URL = "https://duckduckgo.com/html/"
USER_AGENT = "LogsenseStackOverflowTool/1.0"
REQUEST_TIMEOUT_SECONDS = 5
MAX_SNIPPET_LENGTH = 200
_cache: dict[str, list[dict[str, Any]]] = {}


def _search_stackoverflow_impl(query: str, top_k: int = 3) -> list[dict[str, Any]]:
    """Search StackOverflow for relevant discussions and return top results."""
    normalized_query = str(query).strip()
    if not normalized_query or top_k <= 0:
        return []

    cache_key = normalized_query
    if cache_key in _cache:
        return _cache[cache_key][:top_k]

    try:
        response = requests.get(
            f"{DUCKDUCKGO_HTML_URL}?q={quote_plus(f'site:stackoverflow.com {normalized_query}')}",
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        results = _parse_duckduckgo_results(response.text, top_k=top_k)
    except Exception:  # noqa: BLE001
        return []

    _cache[cache_key] = results
    return results[:top_k]


def _parse_duckduckgo_results(html_text: str, top_k: int) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_text, "html.parser")
    collected: list[dict[str, Any]] = []

    for result in soup.select(".result"):
        title_element = result.select_one(".result__title a")
        snippet_element = result.select_one(".result__snippet")
        if title_element is None:
            continue

        raw_url = str(title_element.get("href", "")).strip()
        resolved_url = _resolve_result_url(raw_url)
        if not _is_stackoverflow_url(resolved_url):
            continue

        rank = len(collected) + 1
        collected.append(
            {
                "title": _clean_text(title_element.get_text(" ", strip=True)),
                "url": resolved_url,
                "snippet": _truncate_text(
                    _clean_text(
                        snippet_element.get_text(" ", strip=True) if snippet_element is not None else ""
                    ),
                    MAX_SNIPPET_LENGTH,
                ),
                "score": round(1.0 / rank, 4),
            }
        )
        if len(collected) >= top_k:
            break

    return collected


def _resolve_result_url(raw_url: str) -> str:
    candidate = html.unescape(raw_url).strip()
    if candidate.startswith("//"):
        candidate = f"https:{candidate}"
    if candidate.startswith("/"):
        candidate = urljoin(DUCKDUCKGO_HTML_URL, candidate)
    return candidate


def _is_stackoverflow_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:  # noqa: BLE001
        return False
    hostname = (parsed.hostname or "").lower()
    if hostname not in {"stackoverflow.com", "www.stackoverflow.com"}:
        return False
    return "/questions/" in parsed.path or parsed.path.startswith("/q/")


def _clean_text(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", str(text))
    cleaned = html.unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _truncate_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3].rstrip()}..."


@tool
def search_stackoverflow(query: str, top_k: int = 3) -> list[dict[str, Any]]:
    """Search StackOverflow for relevant discussions and return top results.

    Use this when the LLM needs fast external troubleshooting references for
    programming or system issues and concise community discussion links are enough.
    Returns a list of dictionaries with StackOverflow title, URL, snippet, and score.
    """
    return _search_stackoverflow_impl(query=query, top_k=top_k)


if __name__ == "__main__":
    results = _search_stackoverflow_impl("ssh login failed ubuntu")
    for result in results:
        print(ascii(result))
