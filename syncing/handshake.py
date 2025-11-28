# syncing/handshake.py
import numpy as np
from logging import getLogger
from typing import List, Tuple
from .exceptions import HandshakeError
from .config import SyncConfig

logger = getLogger("syncing")


class HandshakeDetector:
    """
    Detects handshake start/stop sequences based on time differences
    between consecutive timestamps.
    """

    def __init__(self, cfg: SyncConfig):
        """
        Args:
            cfg: SyncConfig dataclass loaded from TOML
        """
        self.start_sequences = cfg.handshake_start_sequences  # dict from TOML
        self.stop_sequences = cfg.handshake_stop_sequences    # dict from TOML

    def find(
        self,
        timestamps: np.ndarray,
        tolerance: int = 1000
    ) -> List[Tuple[Tuple[int, int], Tuple[int, int]]]:
        """
        Find handshake start/stop sequence index ranges in the timestamp array.

        Returns:
            List of tuples:
                [ ((start_begin, start_end), (stop_begin, stop_end)), ... ]
        """
        if len(timestamps) < 2:
            raise ValueError("Not enough timestamps for handshake detection")

        diffs = np.diff(timestamps).astype(np.int32)
        logger.debug(f"Computing diffs between {len(timestamps)} timestamps")

        start_indices = []
        stop_indices = []
        start_names = []
        stop_names = []

        covered_stop = set()
        covered_start = set()

        # Detect STOP sequences first — longer ones get priority
        for name, seq in sorted(self.stop_sequences.items(), key=lambda x: -len(x[1])):
            seq_arr = np.array(seq, dtype=np.int32)
            seq_len = len(seq_arr)

            for i in range(len(diffs) - seq_len + 1):
                if any(j in covered_stop for j in range(i, i + seq_len)):
                    continue

                sub = diffs[i:i + seq_len]
                if np.all(np.abs(sub - seq_arr) <= tolerance):
                    stop_indices.append((i, i + seq_len))
                    covered_stop.update(range(i, i + seq_len))
                    stop_names.append(name)
                    logger.info(f"Stop sequence {name} detected at {i}-{i + seq_len}")

        # Detect START sequences, avoiding overlap with STOp sequences
        for name, seq in sorted(self.start_sequences.items(), key=lambda x: -len(x[1])):
            seq_arr = np.array(seq, dtype=np.int32)
            seq_len = len(seq_arr)

            for i in range(len(diffs) - seq_len + 1):
                if i in covered_start:
                    continue
                if any(j in covered_stop for j in range(i, i + seq_len)):
                    continue

                sub = diffs[i:i + seq_len]
                if np.all(np.abs(sub - seq_arr) <= tolerance):
                    start_indices.append((i, i + seq_len))
                    covered_start.update(range(i, i + seq_len))
                    start_names.append(name)
                    logger.info(f"Start sequence {name} detected at {i}-{i + seq_len}")

        if not start_indices or not stop_indices:
            raise HandshakeError(
                f"No valid sequences found: start={len(start_indices)}, stop={len(stop_indices)}"
            )
        if len(start_indices) != len(stop_indices):
            raise HandshakeError(
                f"Mismatch in sequence counts: start={len(start_indices)}, stop={len(stop_indices)}"
            )

        # Sort and pair starts with stops
        start_sorted = sorted(zip(start_indices, start_names), key=lambda x: x[0])
        stop_sorted = sorted(zip(stop_indices, stop_names), key=lambda x: x[0])

        handshake_pairs = []
        start_names_sorted = []
        stop_names_sorted = []

        for (start_range, sname), (stop_range, tname) in zip(start_sorted, stop_sorted):
            if start_range[1] < stop_range[0]:
                handshake_pairs.append((start_range, stop_range))
                start_names_sorted.append(sname)
                stop_names_sorted.append(tname)
            else:
                raise HandshakeError(
                    f"Stop sequence before start sequence: start_end={start_range[1]}, stop_begin={stop_range[0]}"
                )

        logger.info(f"Detected {len(handshake_pairs)} handshake pairs")
        return handshake_pairs, start_names_sorted, stop_names_sorted