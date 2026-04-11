"""LangGraph-based agentic RAG orchestration for Logsense."""

__all__ = [
    "GroqJSONLLM",
    "load_anomalies",
    "run_pipeline",
    "run_pipeline_with_groq",
]


def __getattr__(name: str):
    if name in __all__:
        from app.agent import runner

        return getattr(runner, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
