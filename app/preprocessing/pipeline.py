from .parser import parse_templates
from .anomaly import detect_anomalies


def run_logai(log_lines):
    """
    Full LogAI pipeline:
    raw logs → templates → anomalies
    """

    parsed_logs = parse_templates(log_lines)

    anomaly_results = detect_anomalies(parsed_logs)

    return {
        "parsed_logs": parsed_logs,
        "anomalies": anomaly_results
    }