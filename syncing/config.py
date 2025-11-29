import toml
from dataclasses import dataclass
from typing import Optional, Dict, List, Union

@dataclass
class SyncConfig:
    # Paths
    path_led_dir: str
    path_h5_dir: str
    path_to_results: str
    log_dir: str

    # Processing params
    rec_id_start: Optional[int]
    rec_id_end: Optional[int]
    threshold: int
    log_level: str
    sync_duration_sec: float
    post_stim_phase: Optional[float]

    # Arduino bytes
    bytes: Dict[str, int]

    # Handshake sequences
    handshake_start_sequences: Dict[str, List[int]]
    handshake_stop_sequences: Dict[str, List[int]]

    @classmethod
    def load(cls, toml_path: str) -> 'SyncConfig':
        cfg = toml.load(toml_path)

        paths = cfg["paths"]
        params = cfg["parameters"]

        return cls(
            # Paths
            path_led_dir=paths["path_led_dir"],
            path_h5_dir=paths["path_h5_dir"],
            path_to_results=paths["path_to_results"],
            log_dir=paths.get("log_dir", "./logs"),

            # Params
            rec_id_start=(int(params["rec_id_start"]) if params.get("rec_id_start") else None),
            rec_id_end=(int(params["rec_id_end"]) if params.get("rec_id_end") else None),
            threshold=int(params["threshold"]),
            log_level=params.get("log_level", "INFO"),
            sync_duration_sec=float(params.get("sync_duration_sec", 9)),
            post_stim_phase=(float(params["post_stim_phase"]) if params.get("post_stim_phase") else None),

            # Arduino bytes
            bytes=cfg["arduino"]["bytes"],

            # Handshake sequences
            handshake_start_sequences=cfg["handshake"]["start_sequences"],
            handshake_stop_sequences=cfg["handshake"]["stop_sequences"]
        )