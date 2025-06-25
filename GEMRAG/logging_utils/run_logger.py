import logging
from logging import FileHandler, Formatter
import os

def setup_run_logger(run_id, log_dir="logs/experiments"):
    logger_name = f"policy_ingest.run.{run_id}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    log_path = os.path.join(log_dir, f"{run_id}.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        handler = logging.FileHandler(log_path)
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = True 
    return logger