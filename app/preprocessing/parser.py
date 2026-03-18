import pandas as pd
import sys
from pathlib import Path

from app.config import PARSED_LOGS_PATH

def parse_templates(log_lines, output_path: Path = PARSED_LOGS_PATH):
    """
    Convert raw log lines into structured templates using LogAI
    and save the parsed output to disk.
    """
    try:
        from logai.information_extraction.log_parser import LogParser, LogParserConfig
    except ValueError as exc:
        if "mutable default" not in str(exc):
            raise
        raise RuntimeError(
            "LogAI is not compatible with the active Python interpreter. "
            f"You are running Python {sys.version_info.major}.{sys.version_info.minor}. "
            "Use the project virtual environment interpreter instead: "
            r".\.venv\Scripts\python.exe .\app\main.py"
        ) from exc

    logline_series = pd.Series(log_lines, name="logline")
    parser = LogParser(LogParserConfig())
    parsed_logs = parser.fit_parse(logline_series)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    parsed_logs.to_csv(output_path, index=False)
    return parsed_logs
