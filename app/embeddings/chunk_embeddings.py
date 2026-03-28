import json
import os
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from app.config import (
    CHUNKS_PATH,
    COHERE_API_KEY_ENV_VAR,
    COHERE_EMBED_BATCH_SIZE,
    COHERE_EMBED_INPUT_TYPE,
    COHERE_EMBED_MODEL,
    COHERE_EMBEDDING_TYPE,
    COHERE_REQUEST_DELAY_SECONDS,
    EMBED_INPUT_DATASET_PATH,
    FINAL_PATH,
)

EMBED_TEXT_COLUMN = "text"
EMBED_METADATA_COLUMNS = ["chunk_id"]
FINAL_OUTPUT_COLUMNS = [
    "chunk_id",
    "start_row",
    "end_row",
    "process_pid",
    "process_name",
    "description",
    "high_level_description",
    "low_level_description",
    "embedding",
]


def build_final_with_embeddings(
    chunks_csv: str | Path = CHUNKS_PATH,
    output_csv: str | Path = FINAL_PATH,
    dataset_csv: str | Path = EMBED_INPUT_DATASET_PATH,
    model_name: str = COHERE_EMBED_MODEL,
    batch_size: int = COHERE_EMBED_BATCH_SIZE,
    request_delay_seconds: float = COHERE_REQUEST_DELAY_SECONDS,
) -> None:
    """Create final.csv by embedding the low-level chunk descriptions with Cohere."""
    _load_env()
    api_key = os.getenv(COHERE_API_KEY_ENV_VAR)
    if not api_key:
        raise RuntimeError(f"Missing {COHERE_API_KEY_ENV_VAR} in environment.")

    try:
        import cohere
    except ImportError as error:
        raise RuntimeError(
            "Missing dependency 'cohere'. Install it with: .\\.venv\\Scripts\\python.exe -m pip install cohere"
        ) from error

    chunks = pd.read_csv(chunks_csv)
    final_frame = _normalize_chunks(chunks)

    dataset_path = Path(dataset_csv)
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    embed_input = final_frame[["chunk_id", "low_level_description"]].rename(
        columns={"low_level_description": EMBED_TEXT_COLUMN}
    )
    embed_input.to_csv(dataset_path, index=False)

    client = _build_cohere_client(cohere, api_key)
    embedded_records = _embed_with_jobs_or_fallback(
        client=client,
        dataset_path=dataset_path,
        input_rows=embed_input.to_dict(orient="records"),
        model_name=model_name,
        batch_size=batch_size,
        request_delay_seconds=request_delay_seconds,
    )

    embeddings_by_chunk = {
        str(record["chunk_id"]): json.dumps(record["embedding"], ensure_ascii=True)
        for record in embedded_records
    }
    final_frame["embedding"] = final_frame["chunk_id"].astype(str).map(embeddings_by_chunk).fillna("[]")

    destination = Path(output_csv)
    destination.parent.mkdir(parents=True, exist_ok=True)
    final_frame.to_csv(destination, index=False)


def _load_env() -> None:
    app_env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(app_env_path)
    load_dotenv()


def _normalize_chunks(chunks: pd.DataFrame) -> pd.DataFrame:
    frame = chunks.copy()
    for column in FINAL_OUTPUT_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    for column in ("description", "high_level_description", "low_level_description", "embedding"):
        frame[column] = frame[column].fillna("").astype(str)
    frame["process_pid"] = frame["process_pid"].apply(_serialize_pid)
    frame["chunk_id"] = frame["chunk_id"].astype(str)

    missing_low_level = frame["low_level_description"].str.strip() == ""
    if missing_low_level.any():
        missing_chunk_ids = frame.loc[missing_low_level, "chunk_id"].tolist()
        raise ValueError(
            f"low_level_description is missing for chunk(s): {', '.join(missing_chunk_ids)}"
        )

    return frame.reindex(columns=FINAL_OUTPUT_COLUMNS)


def _build_cohere_client(cohere_module, api_key: str):
    if hasattr(cohere_module, "ClientV2"):
        return cohere_module.ClientV2(api_key=api_key)
    return cohere_module.Client(api_key)


def _create_embed_dataset(client, dataset_path: Path):
    dataset = client.datasets.create(
        name=dataset_path.stem,
        data=dataset_path.open("rb"),
        type="embed-input",
        keep_fields=EMBED_METADATA_COLUMNS,
    )
    return client.wait(dataset)


def _create_embed_job(client, dataset_id: str, model_name: str):
    job = client.embed_jobs.create(
        dataset_id=dataset_id,
        input_type=COHERE_EMBED_INPUT_TYPE,
        model=model_name,
        embedding_types=[COHERE_EMBEDDING_TYPE],
        truncate="END",
    )
    return client.wait(job)


def _embed_with_jobs_or_fallback(
    client,
    dataset_path: Path,
    input_rows: list[dict[str, str]],
    model_name: str,
    batch_size: int,
    request_delay_seconds: float,
) -> list[dict[str, object]]:
    try:
        validated_dataset = _create_embed_dataset(client, dataset_path)
        completed_job = _create_embed_job(client, _extract_dataset_id(validated_dataset), model_name)
        return _fetch_embedded_records(client, completed_job)
    except Exception as error:  # noqa: BLE001
        if not _should_fallback_to_direct_embeddings(error):
            raise
        return _embed_with_direct_api(
            client=client,
            input_rows=input_rows,
            model_name=model_name,
            batch_size=batch_size,
            request_delay_seconds=request_delay_seconds,
        )


def _fetch_embedded_records(client, completed_job) -> list[dict[str, object]]:
    output_dataset_id = _extract_output_dataset_id(completed_job)
    dataset_response = client.datasets.get(id=output_dataset_id)
    dataset = getattr(dataset_response, "dataset", dataset_response)
    records_iterable = _extract_dataset_records(dataset)

    records = []
    for record in records_iterable:
        normalized_record = _to_mapping(record)
        embedding_values = _extract_embedding_values(normalized_record)
        records.append(
            {
                "chunk_id": normalized_record.get("chunk_id", ""),
                "embedding": embedding_values,
            }
        )
    return records


def _extract_dataset_id(dataset_response) -> str:
    if hasattr(dataset_response, "id"):
        return dataset_response.id
    if hasattr(dataset_response, "dataset") and hasattr(dataset_response.dataset, "id"):
        return dataset_response.dataset.id
    raise RuntimeError("Unable to determine Cohere dataset id from dataset response.")


def _extract_output_dataset_id(completed_job) -> str:
    if hasattr(completed_job, "output_dataset_id"):
        return completed_job.output_dataset_id
    if hasattr(completed_job, "job") and hasattr(completed_job.job, "output_dataset_id"):
        return completed_job.job.output_dataset_id

    if hasattr(completed_job, "output") and hasattr(completed_job.output, "id"):
        return completed_job.output.id
    if hasattr(completed_job, "job") and hasattr(completed_job.job, "output") and hasattr(completed_job.job.output, "id"):
        return completed_job.job.output.id

    raise RuntimeError("Unable to determine Cohere output dataset id from embed job response.")


def _extract_embedding_values(record: dict[str, object]) -> list[float]:
    embeddings = record.get("embeddings", {})
    if isinstance(embeddings, dict):
        float_values = embeddings.get("float")
        if float_values is not None:
            return list(float_values)

    if "embedding" in record and record["embedding"] is not None:
        return list(record["embedding"])

    raise RuntimeError("Unable to extract float embeddings from Cohere output record.")


def _embed_with_direct_api(
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
            texts=[row[EMBED_TEXT_COLUMN] for row in batch],
            embedding_types=[COHERE_EMBEDDING_TYPE],
            truncate="END",
        )
        embeddings = _extract_direct_embeddings(response)
        for row, embedding in zip(batch, embeddings, strict=True):
            records.append(
                {
                    "chunk_id": row["chunk_id"],
                    "embedding": embedding,
                }
            )
        if start + batch_size < len(input_rows):
            time.sleep(request_delay_seconds)

    return records


def _extract_direct_embeddings(response) -> list[list[float]]:
    if hasattr(response, "embeddings"):
        embeddings = response.embeddings
        if hasattr(embeddings, "float_") and embeddings.float_ is not None:
            return [list(values) for values in embeddings.float_]
        if hasattr(embeddings, "float") and getattr(embeddings, "float") is not None:
            return [list(values) for values in getattr(embeddings, "float")]
    raise RuntimeError("Unable to extract direct Cohere embeddings from response.")


def _should_fallback_to_direct_embeddings(error: Exception) -> bool:
    error_text = str(error).lower()
    return "payment method" in error_text or "forbidden" in error_text


def _extract_dataset_records(dataset) -> list[object]:
    if isinstance(dataset, list):
        return dataset
    if hasattr(dataset, "records"):
        return list(dataset.records)
    if hasattr(dataset, "__iter__"):
        return list(dataset)
    raise RuntimeError("Unable to iterate through Cohere output dataset records.")


def _to_mapping(record) -> dict[str, object]:
    if isinstance(record, dict):
        return record
    if hasattr(record, "model_dump"):
        return record.model_dump()
    if hasattr(record, "__dict__"):
        return dict(record.__dict__)
    raise RuntimeError("Unable to normalize Cohere output record.")


def _serialize_pid(pid_value):
    if pd.isna(pid_value):
        return ""
    if isinstance(pid_value, str):
        return pid_value
    if float(pid_value).is_integer():
        return int(pid_value)
    return pid_value


if __name__ == "__main__":
    build_final_with_embeddings()
