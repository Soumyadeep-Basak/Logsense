import json
import re
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

KB_RAW_INPUT_PATH = Path("app/data/kb_raw/discourse_threads.json")
KB_CHUNKS_OUTPUT_PATH = Path("app/data/kb_processed/kb_chunks.csv")
KB_MIN_TEXT_LENGTH = 80

PROCESS_KEYWORD_MAP = {
    "sshd": ["sshd", "ssh", "openssh", "authorized_keys", "known_hosts"],
    "CRON": ["cron", "crontab", "cronjob", "scheduled task"],
    "kernel": ["kernel", "dmesg", "oops", "panic", "segfault", "oom killer"],
    "rsyslogd": ["rsyslog", "syslog", "rsyslogd", "logging daemon"],
    "sudo": ["sudo", "sudoers", "privilege escalation"],
    "pam": ["pam", "pam_unix", "authentication failure"],
    "NetworkManager": ["networkmanager", "network manager", "nm-dispatcher"],
    "dhclient": ["dhclient", "dhcp", "lease", "ip address"],
    "systemd": ["systemd", "journald", "unit file", "service failed"],
    "combo": ["combo", "invalid user", "failed password"],
}


def html_to_clean_text(html: str) -> str:
    text = BeautifulSoup(html, "html.parser").get_text(separator="\n")
    text = text.strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def extract_kb_process_type(text: str) -> str:
    lower_text = text.lower()
    matches = [
        process_name
        for process_name, keywords in PROCESS_KEYWORD_MAP.items()
        if any(keyword.lower() in lower_text for keyword in keywords)
    ]
    return ",".join(matches) if matches else "general"


def build_kb_chunks(raw_path=KB_RAW_INPUT_PATH) -> pd.DataFrame:
    raw_threads = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    rows = []

    for thread in raw_threads:
        cleaned_posts = [html_to_clean_text(post_html) for post_html in thread.get("kb_posts_html", [])]
        cleaned_posts = [post for post in cleaned_posts if post.strip()]
        if not cleaned_posts:
            continue

        question_text = cleaned_posts[0]
        replies_text = "\n\n".join(cleaned_posts[1:])
        title = str(thread.get("kb_title") or "")

        if replies_text.strip():
            kb_full_text = f"QUESTION: {title}\n{question_text}\n\nANSWERS:\n{replies_text}"
        else:
            kb_full_text = f"QUESTION: {title}\n{question_text}"

        if len(kb_full_text.strip()) < KB_MIN_TEXT_LENGTH:
            continue

        rows.append(
            {
                "kb_chunk_id": f"kb_discourse_{thread['kb_thread_id']}",
                "kb_source": "ubuntu_discourse",
                "kb_url": str(thread.get("kb_url") or ""),
                "kb_title": title,
                "kb_process_type": extract_kb_process_type(kb_full_text),
                "kb_category_id": int(thread.get("kb_category_id") or 0),
                "kb_views": int(thread.get("kb_views") or 0),
                "kb_reply_count": int(thread.get("kb_reply_count") or 0),
                "kb_text": kb_full_text,
            }
        )

    columns = [
        "kb_chunk_id",
        "kb_source",
        "kb_url",
        "kb_title",
        "kb_process_type",
        "kb_category_id",
        "kb_views",
        "kb_reply_count",
        "kb_text",
    ]
    frame = pd.DataFrame(rows, columns=columns)
    if frame.empty:
        return frame
    return frame.sort_values("kb_views", ascending=False).reset_index(drop=True)


def save_kb_chunks(df: pd.DataFrame, output_path=KB_CHUNKS_OUTPUT_PATH) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path


def run_chunker() -> Path:
    raw_threads = json.loads(KB_RAW_INPUT_PATH.read_text(encoding="utf-8"))
    df = build_kb_chunks(KB_RAW_INPUT_PATH)
    output_path = save_kb_chunks(df)

    print(f"Total threads loaded: {len(raw_threads)}")
    print(f"Chunks produced: {len(df)}")
    if df.empty:
        print("Chunks per process_type: none")
    else:
        counts = df["kb_process_type"].value_counts()
        print("Chunks per process_type:")
        for process_type, count in counts.items():
            print(f"  {process_type}: {count}")
    return output_path


if __name__ == "__main__":
    run_chunker()
