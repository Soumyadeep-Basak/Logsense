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
# GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# GROK_API_KEY = os.getenv("GROK_API_KEY")
ONE_CLASS_SVM_MODEL_PATH = BASE_DIR / "models" / "ocsvm_model.joblib"
