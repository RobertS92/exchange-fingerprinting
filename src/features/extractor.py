# src/features/extractor.py
from typing import Dict
import pandas as pd

from .interarrival import interarrival_features
from .msg_ngrams import msgtype_ngrams
from .ack_latency import ack_latency_features
from .seq_anomaly import seqnum_anomaly_features

def extract_features(window_df: pd.DataFrame) -> Dict[str, float]:
    feats: Dict[str, float] = {}
    feats.update(interarrival_features(window_df, max_fft_bins=20))
    feats.update(msgtype_ngrams(window_df, ngram_range=(1, 2)))
    feats.update(ack_latency_features(window_df))
    feats.update(seqnum_anomaly_features(window_df))
    return feats
