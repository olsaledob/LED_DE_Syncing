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
      2. Detailed correction log mapping original entries to new timestamps with anomaly info
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

        # --- Prepare anomaly lookup ---
        anomaly_map: Dict[int, str] = {idx: t for idx, t in self.anomalies}

        # --- Prepare handshake marking ---
        handshake_map: Dict[int, str] = {}
        if handshake_pairs:
            for (start_range, stop_range) in handshake_pairs:
                start_ts_indices = range(start_range[0], start_range[1]+1)
                stop_ts_indices = range(stop_range[0], stop_range[1]+1)
                for idx in start_ts_indices:
                    handshake_map[idx] = "start_handshake"
                for idx in stop_ts_indices:
                    handshake_map[idx] = "stop_handshake"

        # --- Global overflow fix ---
        offset = 0
        overflow_info = []
        for idx in range(len(arduino_ts)):
            if idx > 0 and arduino_ts[idx] < arduino_ts[idx - 1]:
                offset += 2**32
                overflow_flag = True
                logger.debug(f"Arduino overflow detected at index {idx}, adding offset {offset}")
            else:
                overflow_flag = False
            arduino_ts[idx] += offset
            overflow_info.append((overflow_flag, offset))

        # --- Correction data ---
        corrected_ts = []
        corrected_patterns = []
        correction_records = []

        d_e_index = 0
        prev_orig_ts = None
        prev_corr_ts = None

        success = True

        try: 
            for i, (orig_ts, pat, lt) in enumerate(zip(arduino_ts, arduino_patterns, arduino_linetypes)):
                anomaly_type = anomaly_map.get(i, None)

                # record template
                record = {
                    "original_index": i,
                    "original_timestamp": orig_ts,
                    "original_linetype": lt,
                    "anomaly_type": anomaly_type if anomaly_type else "normal",
                    "handshake_phase": handshake_map.get(i, "none"),
                    "overflow_fix": overflow_info[i][0],
                    "overflow_offset": overflow_info[i][1],
                    "mea_indices_used": [],
                    "skip_count": 0,
                    "extra_inserts": 0,
                    "delta_original": None,
                    "delta_corrected": None,
                    "delta_diff_change": None
                }

                # calculate original delta
                if prev_orig_ts is not None:
                    record["delta_original"] = orig_ts - prev_orig_ts

                # ---- anomaly handling ----
                if anomaly_type is None:
                    corr_ts = mea_ts[d_e_index]
                    record["mea_indices_used"].append(d_e_index)
                    d_e_index += 1

                elif anomaly_type == "merge":
                    # Map current Arduino entry to current MEA timestamp
                    corr_ts = mea_ts[d_e_index]
                    record["mea_indices_used"].append(d_e_index)
                    # Skip one MEA entry because merge combines two Arduino events
                    record["skip_count"] = 1
                    d_e_index += 2

                elif anomaly_type == "split_2":
                    # First synthetic timestamp
                    first_corr_ts = mea_ts[d_e_index - 1] + (orig_ts - arduino_ts[i - 1])
                    corrected_ts.append(first_corr_ts)
                    corrected_patterns.append(pat)
                    # Record synthetic timestamp separately
                    correction_records.append({
                        **record,
                        "corrected_timestamp": first_corr_ts,
                        "extra_inserts": 1,
                        "mea_indices_used": [d_e_index - 1],
                        "delta_original": record["delta_original"],
                        "delta_corrected": (first_corr_ts - prev_corr_ts) if prev_corr_ts is not None else None
                    })

                    # Second actual MEA timestamp
                    second_corr_ts = mea_ts[d_e_index]
                    corrected_ts.append(second_corr_ts)
                    corrected_patterns.append(arduino_patterns[i + 1])
                    correction_records.append({
                        **record,
                        "corrected_timestamp": second_corr_ts,
                        "extra_inserts": 0,
                        "mea_indices_used": [d_e_index],
                        "delta_original": None,
                        "delta_corrected": None
                    })

                    d_e_index += 1
                    prev_orig_ts = orig_ts
                    prev_corr_ts = second_corr_ts
                    continue  # we already appended two records

                elif anomaly_type == "split_3":
                    # first synthetic
                    first_corr_ts = mea_ts[d_e_index - 1] + (orig_ts - arduino_ts[i - 1])
                    corrected_ts.append(first_corr_ts)
                    corrected_patterns.append(pat)
                    correction_records.append({
                        **record,
                        "corrected_timestamp": first_corr_ts,
                        "extra_inserts": 2,
                        "mea_indices_used": [d_e_index - 1]
                    })

                    # second synthetic
                    second_corr_ts = mea_ts[d_e_index - 1] + \
                                    (orig_ts - arduino_ts[i - 1]) + \
                                    (arduino_ts[i + 1] - arduino_ts[i])
                    corrected_ts.append(second_corr_ts)
                    corrected_patterns.append(arduino_patterns[i + 1])
                    correction_records.append({
                        **record,
                        "corrected_timestamp": second_corr_ts,
                        "extra_inserts": 1,
                        "mea_indices_used": [d_e_index - 1]
                    })

                    # final actual MEA timestamp
                    third_corr_ts = mea_ts[d_e_index]
                    corrected_ts.append(third_corr_ts)
                    corrected_patterns.append(arduino_patterns[i + 2])
                    correction_records.append({
                        **record,
                        "corrected_timestamp": third_corr_ts,
                        "extra_inserts": 0,
                        "mea_indices_used": [d_e_index]
                    })

                    d_e_index += 1
                    prev_orig_ts = orig_ts
                    prev_corr_ts = third_corr_ts
                    continue

                elif anomaly_type == "pause" or anomaly_type == "unclassified":
                    corr_ts = mea_ts[d_e_index]
                    record["mea_indices_used"].append(d_e_index)
                    d_e_index += 1

                else:
                    # unknown anomaly type — treat as normal
                    corr_ts = mea_ts[d_e_index]
                    record["mea_indices_used"].append(d_e_index)
                    d_e_index += 1

                # assign corrected delta
                if prev_corr_ts is not None:
                    record["delta_corrected"] = corr_ts - prev_corr_ts
                    if record["delta_original"] is not None:
                        record["delta_diff_change"] = record["delta_corrected"] - record["delta_original"]

                record["corrected_timestamp"] = corr_ts

                corrected_ts.append(corr_ts)
                corrected_patterns.append(pat)
                correction_records.append(record)

                prev_orig_ts = orig_ts
                prev_corr_ts = corr_ts
        
        except Exception as e:
            success = False
            logger.exception(f"Error during anomaly fixing at index {i}: {e}")
            # Fill remainder of correction_records with placeholders
            for j in range(i, len(arduino_ts)):
                correction_records.append({
                    "original_index": j,
                    "original_timestamp": arduino_ts[j],
                    "original_linetype": arduino_linetypes[j],
                    "anomaly_type": "not_fixed_due_to_error",
                    "handshake_phase": handshake_map.get(j, "none"),
                    "overflow_fix": overflow_info[j][0],
                    "overflow_offset": overflow_info[j][1],
                    "mea_indices_used": [],
                    "skip_count": None,
                    "extra_inserts": None,
                    "delta_original": None,
                    "delta_corrected": None,
                    "delta_diff_change": None,
                    "corrected_timestamp": None
                })
        if not success:
            base_npz = os.path.splitext(output_npz_path)[0] + "_partial.npz"
            base_csv = os.path.splitext(correction_log_path)[0] + "_partial.csv"
        else:
            base_npz = output_npz_path
            base_csv = correction_log_path

        np.savez_compressed(base_npz,
            timestamps=np.array(corrected_ts, dtype=np.int64),
            patterns=np.array(corrected_patterns, dtype=object)
        )
        if success:
            logger.info(f"Saved corrected data to {base_npz}")
        else:
            logger.info(f"Saved PARTIAL corrected data to {base_npz}")

        df = pd.DataFrame(correction_records)
        df.to_csv(base_csv, index=False)
        if success:
            logger.info(f"Saved correction log to {base_csv}")
        else:
            logger.info(f"Saved PARTIAL correction log to {base_csv}")