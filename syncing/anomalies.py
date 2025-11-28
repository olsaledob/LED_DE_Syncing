import numpy as np
from logging import getLogger
from typing import List, Tuple, Dict
from .exceptions import UnclassifiedAnomalyError, TimeDiffComparisonError

logger = getLogger("syncing")

class AnomalyHandler:
    def __init__(self, expected_diff: int, threshold: int, pause_threshold: int = 300_000):
        self.expected_diff = expected_diff
        self.threshold = threshold
        self.pause_threshold = pause_threshold

    def detect_anomalies(self, timestamps: np.ndarray) -> List[Tuple[int, str]]:
        """
        Detect anomalies in Arduino timestamps relative to expected_diff.
        Returns list of (index, anomaly_type).
        """
        diffs = np.diff(timestamps).astype(np.int64)
        anomalies = []

        for i, dt in enumerate(diffs):
            if dt < 0:
                logger.debug(f"Overflow at index {i}, fixing by adding 2**32")
                diffs[i] += 2**32
                anomalies.append((i, "overflow"))

            elif abs(dt - self.expected_diff) <= self.threshold:
                continue  # normal event

            elif abs(dt - 2 * self.expected_diff) <= self.threshold:
                anomalies.append((i, "merge"))

            elif abs(sum(diffs[i:i+2]) - self.expected_diff) <= self.threshold:
                anomalies.append((i, "split_2"))

            elif abs(sum(diffs[i:i+3]) - self.expected_diff) <= self.threshold:
                anomalies.append((i, "split_3"))

            elif dt > self.pause_threshold:
                anomalies.append((i, "pause"))

            else:
                anomalies.append((i, "unclassified"))
                logger.warning(f"Unclassified anomaly at {i}: dt={dt}")

        logger.info(f"Detected {len(anomalies)} anomalies")
        return anomalies