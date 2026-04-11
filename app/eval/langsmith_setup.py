from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from langsmith import Client
import requests

try:
    from langchain_core.tracers.langchain import LangChainTracer
except ImportError:  # pragma: no cover - compatibility fallback
    from langchain.callbacks.tracers import LangChainTracer

LANGSMITH_PROJECT = "Logsense"
LANGSMITH_API_KEY_ENV_VAR = "LANGCHAIN_API_KEY"
LANGSMITH_ENDPOINT_ENV_VAR = "LANGCHAIN_ENDPOINT"
LANGSMITH_DEFAULT_ENDPOINT = "https://api.smith.langchain.com"


def setup_langsmith():
    """
    Initialize LangSmith tracing using environment variables.
    """
    _load_env()
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ.setdefault("LANGCHAIN_PROJECT", LANGSMITH_PROJECT)

    api_key = os.getenv(LANGSMITH_API_KEY_ENV_VAR, "").strip()
    if not api_key:
        print("Warning: LangSmith tracing disabled because LANGCHAIN_API_KEY is not set.")
        return None

    endpoint = os.getenv(LANGSMITH_ENDPOINT_ENV_VAR, LANGSMITH_DEFAULT_ENDPOINT).rstrip("/")
    if not _langsmith_endpoint_is_reachable(endpoint, api_key):
        os.environ["LANGCHAIN_TRACING_V2"] = "false"
        print(
            "Warning: LangSmith tracing disabled because the API is unreachable. "
            f"Endpoint: {endpoint}"
        )
        return None

    client = Client(api_key=api_key, api_url=endpoint)
    print(
        f"LangSmith tracing enabled for project '{os.environ['LANGCHAIN_PROJECT']}'. "
        "Open https://smith.langchain.com and check the project traces."
    )
    return LangChainTracer(project_name=os.environ["LANGCHAIN_PROJECT"], client=client)


def _load_env() -> None:
    app_env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(app_env_path)
    load_dotenv()


def _langsmith_endpoint_is_reachable(endpoint: str, api_key: str) -> bool:
    try:
        response = requests.get(
            f"{endpoint}/info",
            headers={"x-api-key": api_key},
            timeout=5,
        )
        return response.ok
    except requests.RequestException:
        return False
