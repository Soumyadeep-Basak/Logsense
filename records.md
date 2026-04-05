# Records

This file is the project work log for this repository.

## Working Agreement

- Record each meaningful prompt/request.
- Record each code or config change made.
- Record new files created.
- Record important command runs and their outcomes.
- Use this file as the first local reference point for project history before making further changes.
- Keep entries concise, chronological, and easy to scan.

## Entry Template

### YYYY-MM-DD HH:MM TZ
- Prompt:
- Actions:
- Files changed:
- Files created:
- Runs/results:
- Notes:

## History

### 2026-03-28 00:00 IST
- Prompt:
  Build incident chunks from processed logs using anomaly windows, PID-aware expansion, overlap merging, and `chunks.csv` output.
- Actions:
  Implemented chunk building logic in preprocessing.
  Added config path for `chunks.csv`.
  Wired chunk generation into the preprocessing pipeline after anomaly detection.
- Files changed:
  `app/config.py`
  `app/preprocessing/pipeline.py`
- Files created:
  `app/preprocessing/chunks.py`
- Runs/results:
  Generated `app/data/processed/chunks.csv`.
  Verified output had 24 non-overlapping chunks with valid row ranges.
- Notes:
  Supported both `row_id` and the repo’s current `row_no` column.

### 2026-03-29 00:00 IST
- Prompt:
  Use Groq with `llama-3.1-8b-instant` to summarize each chunk’s log lines into separate `high_level_description` and `low_level_description` columns in `chunks.csv`.
- Actions:
  Added a dedicated enrichment module under `app/enrichment`.
  Loaded the Groq API key from `.env`.
  Added strict system-prompted JSON parsing.
  Added retry logic, request spacing, and prompt compaction fallback for oversized requests.
  Centralized Groq model/env settings in `app/config.py`.
- Files changed:
  `app/config.py`
  `requirements.txt`
- Files created:
  `app/enrichment/__init__.py`
  `app/enrichment/chunk_descriptions.py`
- Runs/results:
  Installed `groq`.
  First run failed on Groq token/request-size limits for large chunks.
  Updated the prompt builder to compact row formatting automatically.
  Reran enrichment successfully.
  Verified all 24 chunks had both description columns populated in `app/data/processed/chunks.csv`.
- Notes:
  `process_pid` formatting was normalized back to integer-like strings where possible.

### 2026-03-29 00:00 IST
- Prompt:
  Use Cohere embeddings with `embed-english-light-v3.0` to embed `low_level_description` and write `final.csv` as a copy of `chunks.csv` plus an `embedding` column.
  Move model names, endpoints, and paths into `app/config.py`.
  Use a separate module for embeddings.
- Actions:
  Centralized Cohere and Groq settings in `app/config.py`.
  Added a dedicated embeddings module under `app/embeddings`.
  Added support for Cohere Embed Jobs dataset creation.
  Added automatic fallback to direct batched `embed(...)` calls when Embed Jobs are blocked.
  Added batch sizing and request spacing controls from config.
- Files changed:
  `app/config.py`
  `app/enrichment/chunk_descriptions.py`
  `requirements.txt`
- Files created:
  `app/embeddings/__init__.py`
  `app/embeddings/chunk_embeddings.py`
- Runs/results:
  Installed `cohere`.
  First Embed Jobs run failed because the Cohere account had no valid payment method on file.
  Added automatic fallback to direct batched embeddings.
  Reran successfully and generated `app/data/processed/final.csv`.
  Verified `final.csv` had 24 rows and a populated `embedding` column for every chunk.
- Notes:
  The code still prefers Embed Jobs first and falls back only when needed.

### 2026-04-05 00:00 IST
- Prompt:
  Keep a persistent `records.md` with changes, prompts, created files, and run results; use it as project memory/history going forward.
- Actions:
  Created this repository-level log file and defined a logging format and working agreement.
  Backfilled major prior work items for chunking, Groq enrichment, and Cohere embeddings.
- Files changed:
  None
- Files created:
  `records.md`
- Runs/results:
  No runtime action required for this setup step.
- Notes:
  I can reliably maintain this file during work in this repo, but I cannot permanently change my global system instructions from inside the chat.

### 2026-04-05 00:00 IST
- Prompt:
  Debug the new knowledge-base pipeline after a runtime failure during Cohere embedding.
- Actions:
  Fixed the KB embed input file so Cohere datasets receive a required `text` header while preserving `kb_text` in the KB output schema.
  Reduced scraper noise by stopping category pagination after the first 404 for a missing category.
- Files changed:
  `app/knowledge_base/kb_embedder.py`
  `app/knowledge_base/kb_scraper.py`
  `records.md`
- Files created:
  None
- Runs/results:
  Root cause identified from the reported traceback: Cohere dataset validation rejected `kb_embed_input.csv` because the file header did not include `text`.
- Notes:
  This fix keeps `kb_final.csv` schema unchanged and only alters the intermediate embed-input CSV format to satisfy Cohere.
