import re
import sys
from pathlib import Path

import pandas as pd

from app.config import PARSED_LOGS_PATH


COMMON_LOG_PATTERN = re.compile(
    r"^(?P<month>[A-Z][a-z]{2})\s+"
    r"(?P<day>\d{1,2})\s+"
    r"(?P<time>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<host>\S+)\s+"
    r"(?P<process_name>[^\s\[:(]+)"
    r"(?:\((?P<process_subtype>[^)]*)\))?"
    r"(?:\[(?P<process_pid>\d+)\])?:?"
)


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
    parsed_logs = _add_common_columns(parsed_logs)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    parsed_logs.to_csv(output_path, index=False)
    return parsed_logs


def _add_common_columns(parsed_logs: pd.DataFrame) -> pd.DataFrame:
    enriched_logs = parsed_logs.copy()
    enriched_logs.insert(0, "row_no", range(1, len(enriched_logs) + 1))

    logline_text = enriched_logs["logline"].fillna("").astype(str)
    extracted = logline_text.str.extract(COMMON_LOG_PATTERN)

    enriched_logs["month"] = extracted["month"].fillna("")
    enriched_logs["day"] = pd.to_numeric(extracted["day"], errors="coerce").astype("Int64")
    enriched_logs["time"] = extracted["time"].fillna("")
    enriched_logs["host"] = extracted["host"].fillna("")
    enriched_logs["process_name"] = extracted["process_name"].fillna("")
    enriched_logs["process_subtype"] = extracted["process_subtype"].fillna("")
    enriched_logs["process_pid"] = pd.to_numeric(
        extracted["process_pid"],
        errors="coerce",
    ).astype("Int64")
    enriched_logs["remote_host"] = logline_text.str.extract(r"\brhost=([^\s]+)")[0].fillna("")
    enriched_logs["user_value"] = logline_text.str.extract(r"\buser=([^\s]+)")[0].fillna("")
    enriched_logs["tty"] = logline_text.str.extract(r"\btty=([^\s]+)")[0].fillna("")
    enriched_logs["tt"] = enriched_logs["tty"]

    ordered_columns = [
        "row_no",
        "logline",
        "parsed_logline",
        "parameter_list",
        "month",
        "day",
        "time",
        "host",
        "process_name",
        "process_subtype",
        "process_pid",
        "remote_host",
        "user_value",
        "tty",
        "tt",
    ]
    remaining_columns = [column for column in enriched_logs.columns if column not in ordered_columns]
    return enriched_logs[ordered_columns + remaining_columns]
