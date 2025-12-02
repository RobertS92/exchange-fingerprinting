# src/features/msg_ngrams.py
from typing import Dict, Tuple
from collections import Counter
import pandas as pd

def msgtype_ngrams(window_df: pd.DataFrame, ngram_range: Tuple[int, int] = (1, 2)) -> Dict[str, float]:
    seq = window_df["msg_type"].tolist()
    feats: Dict[str, float] = {}

    # unigrams
    unigram_counts = Counter(seq)
    total = len(seq)
    for k, v in unigram_counts.items():
        feats[f"uni_{k}"] = v / total if total > 0 else 0.0

    # bigrams
    if ngram_range[1] >= 2 and len(seq) >= 2:
        bigrams = [(seq[i], seq[i+1]) for i in range(len(seq)-1)]
        bigram_counts = Counter(bigrams)
        total_bi = len(bigrams)
        for (a, b), v in bigram_counts.items():
            feats[f"bi_{a}_{b}"] = v / total_bi if total_bi > 0 else 0.0

    return feats
