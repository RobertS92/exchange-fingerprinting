# src/features/interarrival.py
from typing import Dict
import numpy as np
import pandas as pd

def interarrival_features(window_df: pd.DataFrame, max_fft_bins: int = 20) -> Dict[str, float]:
    ts = np.sort(window_df["timestamp_ns"].to_numpy())
    ia = np.diff(ts) / 1e3  # microseconds

    feats: Dict[str, float] = {}

    if len(ia) == 0:
        for k in ["mean", "std", "min", "max", "q25", "q50", "q75"]:
            feats[f"ia_{k}"] = 0.0
        return feats

    feats["ia_mean"] = float(np.mean(ia))
    feats["ia_std"]  = float(np.std(ia))
    feats["ia_min"]  = float(np.min(ia))
    feats["ia_max"]  = float(np.max(ia))
    feats["ia_q25"]  = float(np.quantile(ia, 0.25))
    feats["ia_q50"]  = float(np.quantile(ia, 0.50))
    feats["ia_q75"]  = float(np.quantile(ia, 0.75))

    ia_centered = ia - np.mean(ia)
    n = len(ia_centered)
    n_fft = 1 << (n - 1).bit_length()
    ia_padded = np.zeros(n_fft)
    ia_padded[:n] = ia_centered

    mag = np.abs(np.fft.rfft(ia_padded))
    num_bins = min(max_fft_bins, len(mag))
    for k in range(num_bins):
        feats[f"ia_fft_mag_{k}"] = float(mag[k])

    return feats
