# src/streaming/window_utils.py
from collections import deque
from typing import List, Dict

def build_windows_from_buffer(buf: deque, window_size: int, stride: int) -> List[list[Dict]]:
    """
    Build windows from the tail of a deque of message dicts.
    Windows are overlapping slices of length window_size, with given stride.
    """
    windows: List[list[Dict]] = []
    n = len(buf)
    if n < window_size:
        return windows

    msgs = list(buf)
    i = 0
    while i + window_size <= n:
        windows.append(msgs[i:i+window_size])
        i += stride
    return windows
