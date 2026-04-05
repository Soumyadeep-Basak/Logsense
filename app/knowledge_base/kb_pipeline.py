from app.knowledge_base.kb_scraper import run_scraper
from app.knowledge_base.kb_chunker import run_chunker
from app.knowledge_base.kb_embedder import run_kb_embedder


def run_kb_pipeline() -> None:
    print("=== KB Pipeline: Step 1/3 — Scraping Ubuntu Discourse ===")
    run_scraper()
    print("=== KB Pipeline: Step 2/3 — Chunking ===")
    run_chunker()
    print("=== KB Pipeline: Step 3/3 — Embedding ===")
    run_kb_embedder()
    print("=== KB Pipeline complete. Output: app/data/kb_processed/kb_final.csv ===")


if __name__ == "__main__":
    run_kb_pipeline()
