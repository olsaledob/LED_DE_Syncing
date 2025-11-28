# syncing/arduino_led.py
import os
import numpy as np
from tqdm import tqdm
from logging import getLogger
from .exceptions import LedLogNotFoundError, LedLogNoValidDataError
from .config import SyncConfig

logger = getLogger("syncing")


class ArduinoLEDLogs:
    def __init__(self, filepath: str, cfg: SyncConfig):
        """
        Args:
            filepath: Path to LED log file (.txt)
            cfg: SyncConfig dataclass with bytes section loaded from TOML
        """
        self.filepath = filepath
        self.bytes_cfg = cfg.bytes  # dict: {"BYTE_V": 86, "BYTE_W": 87, ...}
        self.timestamps: np.ndarray | None = None
        self.patterns: list[list[int]] | None = None
        self.linetypes: list[int] | None = None
        self.multiplexing_lines: list[list] | None = None

    @staticmethod
    def parse_arduino_line(line: str) -> list[int]:
        """Splits a semicolon-separated line into a list of ints."""
        return [int(entry) for entry in line.split(";")]

    @staticmethod
    def compose_timestamp(row: list[int], multiplex: bool = False) -> np.uint64:
        """Extracts timestamp from an Arduino log row."""
        if not multiplex:
            return np.frombuffer(bytes(row[1:5]), dtype=np.uint32)[0].astype(np.uint64)
        else:
            return np.frombuffer(bytes(row[5:9]), dtype=np.uint32)[0].astype(np.uint64)

    def load(self):
        """Reads LED log and extracts timestamps, patterns, linetypes, multiplexing lines."""
        logger.info(f"Loading Arduino LED log: {self.filepath}")

        if not os.path.isfile(self.filepath):
            raise LedLogNotFoundError(f"LED log file not found: {self.filepath}")

        timestamps = []
        byte_patterns = []
        linetypes = []
        multiplex_blocks = []
        current_mp_block = []

        try:
            with open(self.filepath, 'r') as file:
                for line in tqdm(file, desc=f"Reading {os.path.basename(self.filepath)}"):
                    row = self.parse_arduino_line(line.strip())
                    last_byte = row[-1]

                    # Pattern lines (V, W, X, Y types)
                    if last_byte in {
                        self.bytes_cfg["BYTE_V"],
                        self.bytes_cfg["BYTE_W"],
                        self.bytes_cfg["BYTE_X"],
                        self.bytes_cfg["BYTE_Y"],
                    }:
                        timestamps.append(self.compose_timestamp(row))
                        byte_patterns.append(row[6:-1])
                        linetypes.append(last_byte)

                        multiplex_blocks.append(current_mp_block)
                        current_mp_block = []

                    # Multiplexing lines (Z type)
                    elif last_byte == self.bytes_cfg["BYTE_Z"]:
                        if len(row) != 16:
                            logger.debug(f"Skipping malformed Z-line: {row}")
                            continue
                        ft_int = np.frombuffer(bytes(row[1:5]), dtype=np.uint32)[0].astype(np.uint64)
                        st_int = np.frombuffer(bytes(row[5:9]), dtype=np.uint32)[0].astype(np.uint64)
                        index_byte = int(row[9])
                        mp_entry = [ft_int, st_int, index_byte]
                        current_mp_block.append(mp_entry)

                    else:
                        # Ignore all other line types
                        continue

        except Exception as e:
            logger.exception(f"Error while parsing LED log: {self.filepath}")
            raise

        if not timestamps:
            raise LedLogNoValidDataError(f"No valid LED timestamps in Arduino log: {self.filepath}")

        logger.info(f"Extracted {len(timestamps)} LED timestamps from {self.filepath}")

        self.timestamps = np.array(timestamps, dtype=np.int64)
        self.patterns = byte_patterns
        self.linetypes = linetypes
        self.multiplexing_lines = multiplex_blocks

        return self