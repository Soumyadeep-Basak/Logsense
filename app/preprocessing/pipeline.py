from .parser import parse_templates
from .anomaly import detect_anomalies
from .chunks import build_chunks
from app.config import ANOMALIES_PATH, CHUNKS_PATH


def run_logai(log_lines):
    """
    Full LogAI pipeline:
    raw logs → templates → anomalies
    """

    parsed_logs = parse_templates(log_lines)

    anomaly_results = detect_anomalies(parsed_logs)
    build_chunks(ANOMALIES_PATH, CHUNKS_PATH)

    return {
        "parsed_logs": parsed_logs,
        "anomalies": anomaly_results
    }
