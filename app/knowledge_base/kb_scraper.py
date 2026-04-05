import json
import time
from pathlib import Path

import requests

KB_DISCOURSE_BASE = "https://discourse.ubuntu.com"
KB_CATEGORY_IDS = [5, 17, 122]
KB_REQUEST_DELAY = 0.6
KB_MAX_PAGES_PER_CAT = 50
KB_RAW_OUTPUT_PATH = Path("app/data/kb_raw/discourse_threads.json")

KB_LOG_KEYWORDS = {
    "syslog",
    "sshd",
    "cron",
    "kernel",
    "journald",
    "rsyslog",
    "oom",
    "out of memory",
    "segfault",
    "systemd",
    "auth.log",
    "dmesg",
    "failed",
    "permission denied",
    "/var/log",
    "pam",
    "sudo",
    "networkmanager",
    "dhclient",
    "watchdog",
    "kernel panic",
}

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


def scrape_discourse() -> list[dict]:
    session = requests.Session()
    all_threads: list[dict] = []

    for category_id in KB_CATEGORY_IDS:
        print(f"Scraping category {category_id}...")
        category_threads: list[dict] = []

        for page_number in range(KB_MAX_PAGES_PER_CAT):
            category_url = f"{KB_DISCOURSE_BASE}/c/{category_id}.json?page={page_number}"

            try:
                category_payload = _get_json(session, category_url)
            except requests.HTTPError as error:
                if error.response is not None and error.response.status_code == 404:
                    print(f"Warning: category {category_id} not found; skipping remaining pages")
                    break
                print(f"Warning: failed category page {category_id}:{page_number} - {error}")
                continue
            except Exception as error:  # noqa: BLE001
                print(f"Warning: failed category page {category_id}:{page_number} - {error}")
                continue

            topics = category_payload.get("topic_list", {}).get("topics", [])
            if not topics:
                break

            for topic in topics:
                if not _is_relevant_topic(topic):
                    continue

                topic_id = topic.get("id")
                if topic_id is None:
                    continue

                topic_url = f"{KB_DISCOURSE_BASE}/t/{topic_id}.json"
                try:
                    topic_payload = _get_json(session, topic_url)
                except Exception as error:  # noqa: BLE001
                    print(f"Warning: failed thread {topic_id} - {error}")
                    continue

                posts = topic_payload.get("post_stream", {}).get("posts", [])
                raw_thread = {
                    "kb_thread_id": int(topic_id),
                    "kb_title": str(topic_payload.get("title") or topic.get("title") or ""),
                    "kb_url": f"{KB_DISCOURSE_BASE}/t/{topic_id}",
                    "kb_category_id": int(topic_payload.get("category_id") or category_id),
                    "kb_views": int(topic_payload.get("views") or topic.get("views") or 0),
                    "kb_reply_count": int(
                        topic_payload.get("reply_count") or topic.get("reply_count") or 0
                    ),
                    "kb_posts_html": [str(post.get("cooked") or "") for post in posts],
                }
                category_threads.append(raw_thread)

        print(f"Threads found in category {category_id}: {len(category_threads)}")
        all_threads.extend(category_threads)

    return all_threads


def save_raw_threads(threads: list[dict]) -> Path:
    KB_RAW_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    KB_RAW_OUTPUT_PATH.write_text(json.dumps(threads, indent=2), encoding="utf-8")
    return KB_RAW_OUTPUT_PATH


def run_scraper() -> Path:
    threads = scrape_discourse()
    output_path = save_raw_threads(threads)
    print(f"Total threads saved: {len(threads)}")
    print(f"Saved raw threads to: {output_path}")
    return output_path


def _get_json(session: requests.Session, url: str) -> dict:
    for attempt in range(2):
        response = None
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 429:
                if attempt == 0:
                    print(f"Warning: rate limited on {url}; sleeping 60 seconds before retry")
                    time.sleep(60)
                    continue
                response.raise_for_status()

            response.raise_for_status()
            return response.json()
        finally:
            time.sleep(KB_REQUEST_DELAY)

    raise RuntimeError(f"Failed to fetch JSON from {url}")


def _is_relevant_topic(topic: dict) -> bool:
    title = str(topic.get("title") or "")
    excerpt = str(topic.get("excerpt") or "")
    haystack = f"{title}\n{excerpt}".lower()
    return any(keyword.lower() in haystack for keyword in KB_LOG_KEYWORDS)


if __name__ == "__main__":
    run_scraper()
