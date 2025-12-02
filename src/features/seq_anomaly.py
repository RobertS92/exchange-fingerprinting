# src/features/seq_anomaly.py
from typing import Dict
import numpy as np
import pandas as pd

def seqnum_anomaly_features(window_df: pd.DataFrame) -> Dict[str, float]:
    feats: Dict[str, float] = {}
    seq = window_df["seq_num"].to_numpy()
    diffs = np.diff(seq)

    if len(diffs) == 0:
        feats["seq_jump_rate"] = 0.0
        feats["seq_backwards_rate"] = 0.0
        return feats

    jumps = diffs > 1
    backwards = diffs <= 0
    feats["seq_jump_rate"] = float(np.mean(jumps))
    feats["seq_backwards_rate"] = float(np.mean(backwards))
    return feats
