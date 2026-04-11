from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
RAW_LOG_PATH = DATA_DIR / "raw" / "Linux_2k.log"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_PATH = PROCESSED_DIR / "processed.json"
PARSED_LOGS_PATH = PROCESSED_DIR / "parsed_logs.csv"
ANOMALIES_PATH = PROCESSED_DIR / "anomalies.csv"
CHUNKS_PATH = PROCESSED_DIR / "chunks.csv"
FINAL_PATH = PROCESSED_DIR / "final.csv"
EMBED_INPUT_DATASET_PATH = PROCESSED_DIR / "embed_input.csv"

GROQ_API_KEY_ENV_VAR = "GROQ_API_KEY"
COHERE_API_KEY_ENV_VAR = "COHERE_API_KEY"

GROQ_API_BASE_URL = "https://api.groq.com/openai/v1"
COHERE_API_BASE_URL = "https://api.cohere.com"

GROQ_INCIDENT_DESCRIPTION_MODEL = "llama-3.1-8b-instant"
COHERE_EMBED_MODEL = "embed-english-light-v3.0"
COHERE_EMBED_INPUT_TYPE = "search_document"
COHERE_EMBEDDING_TYPE = "float"
COHERE_EMBED_BATCH_SIZE = 8
COHERE_REQUEST_DELAY_SECONDS = 2.0

GROQ_REQUEST_DELAY_SECONDS = 4.0
GROQ_MAX_RETRIES = 6

ONE_CLASS_SVM_MODEL_PATH = BASE_DIR / "models" / "ocsvm_model.joblib"
