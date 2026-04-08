from typing import Dict, List, TypedDict


class AgentState(TypedDict):
    query: str
    chunk_id: str
    risk_score: float
    contributor_features: Dict
    context: List[Dict]
    kb_results: List[Dict]
    similar_incidents: List[Dict]
    logs: List[str]
    decision: str
    steps: int
    answer: Dict
