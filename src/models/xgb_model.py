# src/models/xgb_model.py
from typing import Dict, List
import json
import numpy as np
import xgboost as xgb

class ExchangeFingerprintModel:
    def __init__(self, model_path: str, feature_names: List[str]):
        self.model = xgb.XGBClassifier()
        self.model.load_model(model_path)
        self.feature_names = feature_names
        self.classes_ = self.model.classes_

    @classmethod
    def from_config(cls, model_path: str, feature_names_path: str):
        with open(feature_names_path, "r") as f:
            feature_names = json.load(f)
        return cls(model_path, feature_names)

    def predict_proba(self, feature_dict: Dict[str, float]) -> tuple[str, Dict[str, float]]:
        x_vec = np.array([[feature_dict.get(f, 0.0) for f in self.feature_names]])
        proba = self.model.predict_proba(x_vec)[0]
        best_idx = int(np.argmax(proba))
        best_label = str(self.classes_[best_idx])
        prob_map = {str(c): float(p) for c, p in zip(self.classes_, proba)}
        return best_label, prob_map
