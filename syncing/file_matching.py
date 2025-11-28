import os
import re
from .exceptions import LedLogNotFoundError, LedLogNameMismatchError
from logging import getLogger

logger = getLogger("syncing")

def extract_hs_id(filename: str) -> str:
    match = re.search(r'HS\d+', filename)
    return match.group(0) if match else 'UNKNOWN'

def extract_expected_diff_from_filename(filename: str) -> int:
    match = re.search(r"(\d+(?:p\d+|\.\d+)?)ms", filename)
    if not match:
        logger.warning(f"No timestep found in filename {filename}, default 62.5 ms")
        return 62500
    milliseconds = float(match.group(1).replace("p", "."))
    return int(milliseconds * 1000)

def match_led_file(de_filename: str, path_led_dir: str, neighborhood: int = 2) -> str:
    logger.debug(f"Matching LED log for {de_filename}")
    recid_match = re.search(r"RecID[-_]?(\d+)", de_filename)
    if not recid_match:
        logger.info(f"No RecID found in {de_filename}, skipping")
        return None

    recid_num = recid_match.group(1).zfill(3)
    led_candidates = [f for f in os.listdir(path_led_dir) if re.search(rf"RecID[-_]?{recid_num}(?!\d)", f)]

    if len(led_candidates) == 1:
        logger.debug(f"Found single candidate LED log: {led_candidates[0]}")
        return os.path.join(path_led_dir, led_candidates[0])

    if len(led_candidates) > 1:
        logger.warning(f"Multiple LED logs for RecID {recid_num}, taking first match")
        return os.path.join(path_led_dir, led_candidates[0])

    logger.warning(f"No LED log found for RecID {recid_num}, checking neighbors")
    for offset in range(-neighborhood, neighborhood + 1):
        neighbor = str(int(recid_num) + offset).zfill(3)
        neighbor_candidates = [
            f for f in os.listdir(path_led_dir) if re.search(rf"RecID[-_]?{neighbor}(?!\d)", f)
        ]
        if neighbor_candidates:
            return os.path.join(path_led_dir, neighbor_candidates[0])

    raise LedLogNotFoundError(f"No LED log found for RecID-{recid_num} or neighbors.")