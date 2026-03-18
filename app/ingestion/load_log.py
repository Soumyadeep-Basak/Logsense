from pathlib import Path


def load_log_file(file_path: Path):
    """
    Reads a .log file line by line
    """

    logs = []

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                logs.append(line)

    return logs