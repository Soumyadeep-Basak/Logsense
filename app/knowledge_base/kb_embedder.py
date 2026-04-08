import json
import os
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from app.config import (
    COHERE_API_KEY_ENV_VAR,
    COHERE_EMBED_BATCH_SIZE,
    COHERE_EMBED_INPUT_TYPE,
    COHERE_EMBED_MODEL,
    COHERE_EMBEDDING_TYPE,
    COHERE_REQUEST_DELAY_SECONDS,
    GROQ_API_KEY_ENV_VAR,
    GROQ_INCIDENT_DESCRIPTION_MODEL,
    GROQ_MAX_RETRIES,
    GROQ_REQUEST_DELAY_SECONDS,
)

KB_CHUNKS_INPUT_PATH = Path("app/data/kb_processed/kb_chunks.csv")
KB_EMBED_INPUT_PATH = Path("app/data/kb_processed/kb_embed_input.csv")
KB_FINAL_OUTPUT_PATH = Path("app/data/kb_processed/kb_final.csv")

KB_EMBED_TEXT_COLUMN = "kb_text"
KB_EMBED_DATASET_TEXT_COLUMN = "text"
KB_EMBED_ID_COLUMN = "kb_chunk_id"
KB_EMBED_METADATA_COLS = ["kb_chunk_id"]
KB_GROQ_TEXT_MAX_CHARS = 12000
KB_GROQ_REFINED_TEXT_COLUMN = "kb_text"
KB_FINAL_OUTPUT_COLUMNS = [
    "kb_chunk_id",
    "kb_source",
    "kb_url",
    "kb_title",
    "kb_process_type",
    "kb_category_id",
    "kb_views",
    "kb_reply_count",
    "kb_text",
    "kb_embedding",
]


def build_kb_final_with_embeddings(
    chunks_csv=KB_CHUNKS_INPUT_PATH,
    embed_input_csv=KB_EMBED_INPUT_PATH,
    output_csv=KB_FINAL_OUTPUT_PATH,
    model_name=COHERE_EMBED_MODEL,
    batch_size=COHERE_EMBED_BATCH_SIZE,
    request_delay_seconds=COHERE_REQUEST_DELAY_SECONDS,
) -> None:
    chunks = pd.read_csv(chunks_csv)
    final_frame = _kb_normalize_chunks(chunks)

    missing_kb_text = final_frame["kb_text"].str.strip() == ""
    if missing_kb_text.any():
        missing_ids = final_frame.loc[missing_kb_text, "kb_chunk_id"].tolist()
        raise ValueError(f"kb_text is missing for kb_chunk_id(s): {', '.join(missing_ids)}")

    _kb_load_env()
    final_frame = _kb_refine_texts_with_groq(final_frame)

    embed_input = final_frame[[KB_EMBED_ID_COLUMN, KB_EMBED_TEXT_COLUMN]].rename(
        columns={KB_EMBED_TEXT_COLUMN: KB_EMBED_DATASET_TEXT_COLUMN}
    )
    embed_input_path = Path(embed_input_csv)
    embed_input_path.parent.mkdir(parents=True, exist_ok=True)
    embed_input.to_csv(embed_input_path, index=False)

    api_key = os.getenv(COHERE_API_KEY_ENV_VAR)
    if not api_key:
        raise RuntimeError(f"Missing {COHERE_API_KEY_ENV_VAR} in environment.")

    try:
        import cohere
    except ImportError as error:
        raise RuntimeError(
            "Missing dependency 'cohere'. Install it with: .\\.venv\\Scripts\\python.exe -m pip install cohere"
        ) from error

    client = _kb_build_cohere_client(cohere, api_key)
    embedded_records = _kb_embed_with_jobs_or_fallback(
        client=client,
        dataset_path=embed_input_path,
        input_rows=embed_input.to_dict(orient="records"),
        model_name=model_name,
        batch_size=batch_size,
        request_delay_seconds=request_delay_seconds,
    )

    embeddings_by_chunk = {
        str(record["kb_chunk_id"]): json.dumps(record["kb_embedding"], ensure_ascii=True)
        for record in embedded_records
    }
    final_frame["kb_embedding"] = final_frame["kb_chunk_id"].astype(str).map(embeddings_by_chunk).fillna("[]")

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_frame.reindex(columns=KB_FINAL_OUTPUT_COLUMNS).to_csv(output_path, index=False)


def run_kb_embedder() -> None:
    build_kb_final_with_embeddings()
    df = pd.read_csv(KB_FINAL_OUTPUT_PATH)
    print(f"Rows embedded: {len(df)}")
    print(f"Output path: {KB_FINAL_OUTPUT_PATH}")


def _kb_load_env() -> None:
    app_env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(app_env_path)
    load_dotenv()


def _kb_normalize_chunks(chunks: pd.DataFrame) -> pd.DataFrame:
    frame = chunks.copy()
    for column in KB_FINAL_OUTPUT_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    for column in ("kb_source", "kb_url", "kb_title", "kb_process_type", "kb_text", "kb_embedding"):
        frame[column] = frame[column].fillna("").astype(str)
    frame["kb_chunk_id"] = frame["kb_chunk_id"].astype(str)
    return frame.reindex(columns=KB_FINAL_OUTPUT_COLUMNS)


def _kb_refine_texts_with_groq(frame: pd.DataFrame) -> pd.DataFrame:
    groq_api_key = os.getenv(GROQ_API_KEY_ENV_VAR)
    if not groq_api_key:
        raise RuntimeError(f"Missing {GROQ_API_KEY_ENV_VAR} in environment.")

    try:
        from groq import Groq
    except ImportError as error:
        raise RuntimeError(
            "Missing dependency 'groq'. Install it with: .\\.venv\\Scripts\\python.exe -m pip install groq"
        ) from error

    client = Groq(api_key=groq_api_key)
    refined_frame = frame.copy()

    for row_number, row in enumerate(refined_frame.itertuples(index=False), start=1):
        refined_text = _kb_request_refined_text(
            client=client,
            kb_chunk_id=row.kb_chunk_id,
            kb_title=row.kb_title,
            kb_process_type=row.kb_process_type,
            kb_text=row.kb_text,
            model_name=GROQ_INCIDENT_DESCRIPTION_MODEL,
            max_retries=GROQ_MAX_RETRIES,
        )
        refined_frame.at[row_number - 1, KB_GROQ_REFINED_TEXT_COLUMN] = refined_text

        if row_number < len(refined_frame):
            time.sleep(GROQ_REQUEST_DELAY_SECONDS)

    return refined_frame


def _kb_request_refined_text(
    client,
    kb_chunk_id: str,
    kb_title: str,
    kb_process_type: str,
    kb_text: str,
    model_name: str,
    max_retries: int,
) -> str:
    last_error = None
    prompt_modes = ["full", "compact"]

    for prompt_mode in prompt_modes:
        prompt = _kb_build_groq_prompt(
            kb_chunk_id=kb_chunk_id,
            kb_title=kb_title,
            kb_process_type=kb_process_type,
            kb_text=kb_text,
            prompt_mode=prompt_mode,
        )

        for attempt in range(1, max_retries + 1):
            try:
                response = client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {"role": "system", "content": _kb_groq_system_prompt()},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.2,
                    max_completion_tokens=1200,
                    top_p=1,
                    stream=False,
                )
                content = (response.choices[0].message.content or "").strip()
                if content:
                    return content
            except Exception as error:  # noqa: BLE001
                last_error = error
                error_text = str(error)
                if "Request too large" in error_text or "rate_limit_exceeded" in error_text:
                    break
                if attempt == max_retries:
                    break
                time.sleep(attempt * 5)

    raise RuntimeError(f"Failed to refine kb_text with Groq: {last_error}") from last_error


def _kb_build_groq_prompt(
    kb_chunk_id: str,
    kb_title: str,
    kb_process_type: str,
    kb_text: str,
    prompt_mode: str,
) -> str:
    source_text = kb_text if prompt_mode == "full" else _kb_compact_source_text(kb_text)
    metadata = {
        "kb_chunk_id": kb_chunk_id,
        "kb_title": kb_title,
        "kb_process_type": kb_process_type,
        "prompt_mode": prompt_mode,
    }
    return (
        "Refine this Ubuntu knowledge-base thread text for retrieval embedding.\n"
        f"Metadata: {json.dumps(metadata, ensure_ascii=True)}\n"
        "Source text:\n"
        f"{source_text}"
    )


def _kb_groq_system_prompt() -> str:
    return """You clean and compress Ubuntu support knowledge-base text for retrieval embeddings.

Rewrite the text to be shorter and denser while preserving all important Linux/logging concepts.

Rules:
- Return plain text only.
- Keep important process names, services, package names, commands, file paths, config names, error messages, log snippets, and root-cause clues.
- Remove greetings, signatures, repetition, fluff, social text, and irrelevant figures or counts unless they matter technically.
- Preserve the problem, environment, symptoms, attempted fixes, and resolution details when present.
- Do not invent facts.
- Keep important keywords exactly when possible.
"""


def _kb_compact_source_text(text: str) -> str:
    normalized_text = str(text or "").strip()
    if len(normalized_text) <= KB_GROQ_TEXT_MAX_CHARS:
        return normalized_text

    head_chars = 8000
    tail_chars = 3500
    return (
        normalized_text[:head_chars]
        + "\n\n[content truncated for length]\n\n"
        + normalized_text[-tail_chars:]
    )


def _kb_build_cohere_client(cohere_module, api_key: str):
    if hasattr(cohere_module, "ClientV2"):
        return cohere_module.ClientV2(api_key=api_key)
    return cohere_module.Client(api_key)


def _kb_create_embed_dataset(client, dataset_path: Path):
    dataset = client.datasets.create(
        name=dataset_path.stem,
        data=dataset_path.open("rb"),
        type="embed-input",
        keep_fields=KB_EMBED_METADATA_COLS,
    )
    return client.wait(dataset)


def _kb_create_embed_job(client, dataset_id: str, model_name: str):
    job = client.embed_jobs.create(
        dataset_id=dataset_id,
        input_type=COHERE_EMBED_INPUT_TYPE,
        model=model_name,
        embedding_types=[COHERE_EMBEDDING_TYPE],
        truncate="END",
    )
    return client.wait(job)


def _kb_embed_with_jobs_or_fallback(
    client,
    dataset_path: Path,
    input_rows: list[dict[str, str]],
    model_name: str,
    batch_size: int,
    request_delay_seconds: float,
) -> list[dict[str, object]]:
    try:
        validated_dataset = _kb_create_embed_dataset(client, dataset_path)
        completed_job = _kb_create_embed_job(
            client,
            _kb_extract_dataset_id(validated_dataset),
            model_name,
        )
        return _kb_fetch_embedded_records(client, completed_job)
    except Exception as error:  # noqa: BLE001
        if not _kb_should_fallback_to_direct_embeddings(error):
            raise
        return _kb_embed_with_direct_api(
            client=client,
            input_rows=input_rows,
            model_name=model_name,
            batch_size=batch_size,
            request_delay_seconds=request_delay_seconds,
        )


def _kb_fetch_embedded_records(client, completed_job) -> list[dict[str, object]]:
    output_dataset_id = _kb_extract_output_dataset_id(completed_job)
    dataset_response = client.datasets.get(id=output_dataset_id)
    dataset = getattr(dataset_response, "dataset", dataset_response)
    records_iterable = _kb_extract_dataset_records(dataset)

    records = []
    for record in records_iterable:
        normalized_record = _kb_to_mapping(record)
        embedding_values = _kb_extract_embedding_values(normalized_record)
        records.append(
            {
                "kb_chunk_id": normalized_record.get("kb_chunk_id", ""),
                "kb_embedding": embedding_values,
            }
        )
    return records


def _kb_extract_dataset_id(dataset_response) -> str:
    if hasattr(dataset_response, "id"):
        return dataset_response.id
    if hasattr(dataset_response, "dataset") and hasattr(dataset_response.dataset, "id"):
        return dataset_response.dataset.id
    raise RuntimeError("Unable to determine Cohere dataset id from dataset response.")


def _kb_extract_output_dataset_id(completed_job) -> str:
    if hasattr(completed_job, "output_dataset_id"):
        return completed_job.output_dataset_id
    if hasattr(completed_job, "job") and hasattr(completed_job.job, "output_dataset_id"):
        return completed_job.job.output_dataset_id
    if hasattr(completed_job, "output") and hasattr(completed_job.output, "id"):
        return completed_job.output.id
    if hasattr(completed_job, "job") and hasattr(completed_job.job, "output") and hasattr(completed_job.job.output, "id"):
        return completed_job.job.output.id
    raise RuntimeError("Unable to determine Cohere output dataset id from embed job response.")


def _kb_extract_embedding_values(record: dict[str, object]) -> list[float]:
    embeddings = record.get("embeddings", {})
    if isinstance(embeddings, dict):
        float_values = embeddings.get("float")
        if float_values is not None:
            return list(float_values)
    if "embedding" in record and record["embedding"] is not None:
        return list(record["embedding"])
    raise RuntimeError("Unable to extract float embeddings from Cohere output record.")


def _kb_embed_with_direct_api(
    client,
    input_rows: list[dict[str, str]],
    model_name: str,
    batch_size: int,
    request_delay_seconds: float,
) -> list[dict[str, object]]:
    records = []

    for start in range(0, len(input_rows), batch_size):
        batch = input_rows[start : start + batch_size]
        response = client.embed(
            model=model_name,
            input_type=COHERE_EMBED_INPUT_TYPE,
            texts=[row[KB_EMBED_DATASET_TEXT_COLUMN] for row in batch],
            embedding_types=[COHERE_EMBEDDING_TYPE],
            truncate="END",
        )
        embeddings = _kb_extract_direct_embeddings(response)
        for row, embedding in zip(batch, embeddings, strict=True):
            records.append(
                {
                    "kb_chunk_id": row[KB_EMBED_ID_COLUMN],
                    "kb_embedding": embedding,
                }
            )
        if start + batch_size < len(input_rows):
            time.sleep(request_delay_seconds)

    return records


def _kb_extract_direct_embeddings(response) -> list[list[float]]:
    if hasattr(response, "embeddings"):
        embeddings = response.embeddings
        if hasattr(embeddings, "float_") and embeddings.float_ is not None:
            return [list(values) for values in embeddings.float_]
        if hasattr(embeddings, "float") and getattr(embeddings, "float") is not None:
            return [list(values) for values in getattr(embeddings, "float")]
    raise RuntimeError("Unable to extract direct Cohere embeddings from response.")


def _kb_should_fallback_to_direct_embeddings(error: Exception) -> bool:
    error_text = str(error).lower()
    return "payment method" in error_text or "forbidden" in error_text


def _kb_extract_dataset_records(dataset) -> list[object]:
    if isinstance(dataset, list):
        return dataset
    if hasattr(dataset, "records"):
        return list(dataset.records)
    if hasattr(dataset, "__iter__"):
        return list(dataset)
    raise RuntimeError("Unable to iterate through Cohere output dataset records.")


def _kb_to_mapping(record) -> dict[str, object]:
    if isinstance(record, dict):
        return record
    if hasattr(record, "model_dump"):
        return record.model_dump()
    if hasattr(record, "__dict__"):
        return dict(record.__dict__)
    raise RuntimeError("Unable to normalize Cohere output record.")


if __name__ == "__main__":
    run_kb_embedder()
