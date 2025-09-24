import logging, os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_PATH = os.path.join(LOG_DIR, "etl.log")

def get_logger(name="ETLLogger", log_file=LOG_PATH):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)  # <-- FIX here
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger
