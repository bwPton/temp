import logging

def get_logger(name = None):
    return logging.getLogger(name or "policy_ingest")
