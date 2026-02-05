import numpy as np
from typing import List, Tuple

def find_yline_after_start_handshake(arduino_linetypes: List[int], 
                                     handshake_pairs: List[Tuple[Tuple[int, int], Tuple[int, int]]],
                                     byte_y: int):
    
    findings = []
    for k, (start_range, _stop_range) in enumerate(handshake_pairs, start=1):
        sb, se = start_range
        ts_idx = se + 1
        if 0 <= ts_idx < len(arduino_linetypes):
            if arduino_linetypes[ts_idx] == byte_y:
                findings.append({
                    "handshake_id": k,
                    "start_diff_begin": sb,
                    "start_diff_end": se,
                    "first_post_start_ts_index": ts_idx
                })
    return findings
    