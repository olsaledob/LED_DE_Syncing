import logging
import os
from datetime import datetime

def setup_logging(log_dir: str, log_level: str = "INFO") -> logging.Logger:
    """
    Central logging setup. Creates console + file handler.
    
    Args:
        log_dir    : Where to save log files.
        log_level  : Logging level ('DEBUG', 'INFO', etc.).

    Returns:
        logging.Logger instance configured with file + stream handlers.
    """
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"sync_log_{timestamp}.log")

    logger = logging.getLogger("syncing")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Formatter
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(module)s.%(funcName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, log_level.upper()))
    ch.setFormatter(fmt)

    # File handler
    fh = logging.FileHandler(log_file, mode='w')
    fh.setLevel(getattr(logging, log_level.upper()))
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)

    logger.info("Logging initialised")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Log level: {log_level}")

    return logger