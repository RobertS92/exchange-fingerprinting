# api/serve_model.py
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, List

import pandas as pd

from src.config import MODEL_PATH, FEATURE_NAMES_PATH
from src.features.extractor import extract_features
from src.models.xgb_model import ExchangeFingerprintModel

app = FastAPI(title="Exchange Fingerprint Model API")

model = ExchangeFingerprintModel.from_config(
    model_path=MODEL_PATH,
    feature_names_path=FEATURE_NAMES_PATH
)

class Message(BaseModel):
    timestamp_ns: int
    exchange_name: str
    stream_id: str
    msg_type: str
    seq_num: int | None = None
    payload_size: int | None = None

class WindowRequest(BaseModel):
    messages: List[Message]

class PredictionResponse(BaseModel):
    predicted_exchange: str
    probabilities: Dict[str, float]

@app.post("/predict_window", response_model=PredictionResponse)
def predict_window(req: WindowRequest):
    df = pd.DataFrame([m.dict() for m in req.messages])
    feats = extract_features(df)
    label, prob_map = model.predict_proba(feats)
    return PredictionResponse(predicted_exchange=label, probabilities=prob_map)
