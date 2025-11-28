import numpy as np
from typing import List, Tuple

def trim_to_handshake_windows(timestamps: np.ndarray,
                              handshake_pairs: List[Tuple[Tuple[int, int], Tuple[int, int]]]
                              ) -> np.ndarray:
    """
    Remove handshake sequences from timestamps and return only the stimulus-phase timestamps.

    Args:
        timestamps: np.ndarray of timestamps (full session)
        handshake_pairs: list of ((start_begin, start_end), (stop_begin, stop_end))
                         These are INDEX ranges from HandshakeDetector.find()
    Returns:
        np.ndarray of timestamps within stimulus phases only.
    """
    trimmed = []
    for (start_range, stop_range) in handshake_pairs:
        # Handshake indices are diff indices, convert to timestamp indices:
        start_idx = start_range[1]   # timestamp after handshake start ends
        stop_idx = stop_range[0]     # timestamp before handshake stop begins
        trimmed.extend(timestamps[start_idx:stop_idx])
    return np.array(trimmed, dtype=np.int64)

def trim_with_index_mapping(timestamps, handshake_pairs):
    trimmed = []
    index_map = []
    for (start_range, stop_range) in handshake_pairs:
        start_idx = start_range[1]  # first after start handshake ends
        stop_idx = stop_range[0]    # before stop handshake
        trimmed.extend(timestamps[start_idx:stop_idx])
        index_map.extend(range(start_idx, stop_idx))
    return np.array(trimmed, dtype=np.int64), index_map