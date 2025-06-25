import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging(global_logfile = None, level=logging.INFO):
    log_format = "[%(asctime)s] %(levelname)s - %(name)s - %(message)s"
    handlers = [logging.StreamHandler()]

    if global_logfile:
        os.makedirs(os.path.dirname(global_logfile), exist_ok=True)
        file_handler = RotatingFileHandler(global_logfile, maxBytes=1_000_000, backupCount=3)
        handlers.append(file_handler)

    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=handlers
    )
