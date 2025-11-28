import h5py
import numpy as np
from logging import getLogger
from .exceptions import DENotFound

logger = getLogger("syncing")

class DigitalEvents:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.timestamps = None

    def load(self, sync_duration_sec: float = 9, post_stim_phase: float = None):
        logger.info(f"Loading digital events from {self.filepath}")
        try:
            f = h5py.File(self.filepath, 'r')
        except (FileNotFoundError, OSError) as e:
            raise DENotFound(f"Digital events file not found: {self.filepath}") from e

        d_h = np.array(f['/Data/Recording_0/EventStream/Stream_0/EventEntity_0'][0], dtype=np.int64)
        d_l = np.array(f['/Data/Recording_0/EventStream/Stream_1/EventEntity_0'][0], dtype=np.int64)

        d_h = d_h[d_h > sync_duration_sec * 1e6]
        d_l = d_l[d_l > sync_duration_sec * 1e6]

        if post_stim_phase:
            logger.debug("Trimming events beyond post-stim phase")
            d_h = d_h[d_h < post_stim_phase * 1e6]
            d_l = d_l[d_l < post_stim_phase * 1e6]

        d_e = np.empty((d_h.size + d_l.size,), dtype=np.int64)
        if d_h[0] < d_l[0]:
            d_e[0::2] = d_h
            d_e[1::2] = d_l
        else:
            logger.info("Digital events start with low value")
            d_e[1::2] = d_h
            d_e[0::2] = d_l
        
        self.timestamps = d_e
        return self