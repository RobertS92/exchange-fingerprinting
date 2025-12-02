# src/features/ack_latency.py
from typing import Dict, List
import numpy as np
import pandas as pd

def ack_latency_features(window_df: pd.DataFrame) -> Dict[str, float]:
    feats: Dict[str, float] = {}
    dfw = window_df.sort_values("timestamp_ns").reset_index(drop=True)

    last_new_time = None
    lat_ack: List[float] = []
    lat_nack: List[float] = []

    for _, row in dfw.iterrows():
        t = row["timestamp_ns"]
        mt = row["msg_type"]
        if mt == "NEW":
            last_new_time = t
        elif mt in ("ACK", "NACK") and last_new_time is not None:
            dt_us = (t - last_new_time) / 1e3
            if mt == "ACK":
                lat_ack.append(dt_us)
            else:
                lat_nack.append(dt_us)

    def add_stats(prefix: str, arr: List[float]):
        if not arr:
            feats[f"{prefix}_mean"] = 0.0
            feats[f"{prefix}_std"] = 0.0
            return
        arr_np = np.array(arr)
        feats[f"{prefix}_mean"] = float(arr_np.mean())
        feats[f"{prefix}_std"] = float(arr_np.std())

    add_stats("ack_lat", lat_ack)
    add_stats("nack_lat", lat_nack)
    feats["ack_count"] = float(len(lat_ack))
    feats["nack_count"] = float(len(lat_nack))

    return feats
