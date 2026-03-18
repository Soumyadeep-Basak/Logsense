from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import RAW_LOG_PATH
from app.ingestion.load_log import load_log_file
# from app.ingestion.stack_grouping import group_stack_traces


def main():
    if sys.version_info >= (3, 13):
        raise RuntimeError(
            "This project currently needs Python 3.12 for LogAI compatibility. "
            "Run it with the virtual environment interpreter: "
            r".\.venv\Scripts\python.exe .\app\main.py"
        )

    print("Loading logs...")
    from app.preprocessing.pipeline import run_logai

    logs = load_log_file(RAW_LOG_PATH)

    print(f"Loaded {len(logs)} lines")

    # OPTIONAL: group stack traces
    # events = group_stack_traces(logs)

    # print(f"Grouped into {len(events)} events")

    print("Running LogAI...")

    results = run_logai(logs)

    print("\n=== PARSED LOGS ===")
    print(results["parsed_logs"].head())

    print("\n=== ANOMALIES ===")
    print(results["anomalies"].head())


if __name__ == "__main__":
    main()
