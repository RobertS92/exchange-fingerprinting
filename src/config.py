# src/config.py
import os
from dotenv import load_dotenv

load_dotenv()  # loads from .env if present

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

RAW_TOPIC = os.getenv("RAW_TOPIC", "raw_messages")
CLASSIFIED_TOPIC = os.getenv("CLASSIFIED_TOPIC", "classified_windows")

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://user:pass@localhost/exchange_fp")

MODEL_PATH = os.getenv("MODEL_PATH", "data/models/xgb_exchange_fp.json")
FEATURE_NAMES_PATH = os.getenv("FEATURE_NAMES_PATH", "data/metadata/feature_names.json")
