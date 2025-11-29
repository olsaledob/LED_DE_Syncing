# syncing/fix_anomalies.py
import numpy as np
import pandas as pd
import os
from logging import getLogger
from typing import List, Tuple, Dict, Optional

logger = getLogger("syncing")

class AnomalyFixer:
    """
    Corrects anomalies detected in Arduino LED logs and produces:
      1. Corrected timestamps/patterns (.npz)
      2. Correction log mapping original entries to new timestamps with anomaly info
    """

    def __init__(self,
                 anomalies: List[Tuple[int, str]],
                 expected_diff: int,
                 threshold: int):
        self.anomalies = anomalies
        self.expected_diff = expected_diff
        self.threshold = threshold

    def fix(self,
         arduino_ts: np.ndarray,
         arduino_patterns: List[List[int]],
         arduino_linetypes: List[int],
         mea_ts: np.ndarray,
         output_npz_path: str,
         correction_log_path: str,
         handshake_pairs: Optional[List[Tuple[Tuple[int, int], Tuple[int, int]]]] = None
         ) -> None:
        """
        Fix anomalies and produce npz + correction log.

        handshake_pairs: list of ((start_begin, start_end), (stop_begin, stop_end)) in diff-index coords
                        From HandshakeDetector.find() if you want to mark those lines in the log.
        """
        logger.info("Starting anomaly fixing...")

        corrected_ts = []
        corrected_patterns = []
        anomaly_map: Dict[int, str] = {idx: t for idx, t in self.anomalies}

        # --- Prepare handshake marking ---
        handshake_map: Dict[int, str] = {}
        if handshake_pairs:
            for (start_range, stop_range) in handshake_pairs:
                # Convert diff indices to timestamp indices:
                start_ts_indices = range(start_range[0], start_range[1]+1)
                stop_ts_indices = range(stop_range[0], stop_range[1]+1)
                for idx in start_ts_indices:
                    handshake_map[idx] = "start_handshake"
                for idx in stop_ts_indices:
                    handshake_map[idx] = "stop_handshake"

        # Process anomalies
        offset = 0
        for idx in range(len(arduino_ts)):
            if idx > 0 and arduino_ts[idx] < arduino_ts[idx - 1]:
                offset += 2**32
            arduino_ts[idx] += offset

        d_e_index = 0
        for i, (orig_ts, pat, lt) in enumerate(zip(arduino_ts, arduino_patterns, arduino_linetypes)):
            anomaly_type = anomaly_map.get(i, None)

            if anomaly_type is None:
                corrected_ts.append(mea_ts[d_e_index])
                corrected_patterns.append(pat)
                d_e_index += 1

            elif anomaly_type == "merge":
                corrected_ts.append(mea_ts[d_e_index])
                corrected_patterns.append(pat)
                d_e_index += 2

            elif anomaly_type == "split_2":
                corrected_ts.append(mea_ts[d_e_index - 1] + (arduino_ts[i] - arduino_ts[i - 1]))
                corrected_patterns.append(pat)
                corrected_ts.append(mea_ts[d_e_index])
                corrected_patterns.append(arduino_patterns[i + 1])
                d_e_index += 1

            elif anomaly_type == "split_3":
                corrected_ts.append(mea_ts[d_e_index - 1] + (arduino_ts[i] - arduino_ts[i - 1]))
                corrected_patterns.append(pat)
                corrected_ts.append(mea_ts[d_e_index - 1] + 
                                    (arduino_ts[i] - arduino_ts[i - 1]) + 
                                    (arduino_ts[i + 1] - arduino_ts[i]))
                corrected_patterns.append(arduino_patterns[i + 1])
                corrected_ts.append(mea_ts[d_e_index])
                corrected_patterns.append(arduino_patterns[i + 2])
                d_e_index += 1

            elif anomaly_type == "pause":
                corrected_ts.append(mea_ts[d_e_index])
                corrected_patterns.append(pat)
                d_e_index += 1

            elif anomaly_type == "unclassified":
                corrected_ts.append(mea_ts[d_e_index])
                corrected_patterns.append(pat)
                d_e_index += 1

        # Save corrected npz
        np.savez_compressed(
            output_npz_path,
            timestamps=np.array(corrected_ts, dtype=np.int64),
            patterns=np.array(corrected_patterns, dtype=object)
        )
        logger.info(f"Saved corrected data to {output_npz_path}")

        # Build correction log DataFrame
        df = pd.DataFrame({
            "original_index": np.arange(len(arduino_ts)),
            "original_timestamp": arduino_ts,
            "original_linetype": arduino_linetypes,
            "corrected_timestamp": np.array(corrected_ts, dtype=np.int64)[:len(arduino_ts)],
            "anomaly_type": [anomaly_map.get(i, "normal") for i in range(len(arduino_ts))],
            "handshake_phase": [handshake_map.get(i, "none") for i in range(len(arduino_ts))]
        })

        df.to_csv(correction_log_path, index=False)
        logger.info(f"Saved correction log to {correction_log_path}")