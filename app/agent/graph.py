from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from app.agent.nodes import final_node, pre_retrieval_node, reasoning_node, tool_node
from app.agent.state import AgentState


def build_graph(llm, tracer=None):
    builder = StateGraph(AgentState)

    builder.add_node("pre", pre_retrieval_node)
    builder.add_node("reason", lambda state: reasoning_node(state, llm, tracer))
    builder.add_node("tool", tool_node)
    builder.add_node("final", lambda state: final_node(state, llm, tracer))

    builder.add_edge(START, "pre")
    builder.add_edge("pre", "reason")

    builder.add_conditional_edges(
        "reason",
        _route_reasoning,
        {
            "expand_context": "tool",
            "similar_incidents": "tool",
            "search_kb": "tool",
            "stackoverflow": "tool",
            "answer": "final",
        },
    )

    builder.add_edge("tool", "reason")
    builder.add_edge("final", END)

    return builder.compile()


def _route_reasoning(state: AgentState) -> str:
    return str(state.get("decision", "answer"))
