### logger.py

import logging
import logging.handlers
import os
from gis_tool import config

def setup_logging(log_file: str = None, level: int = None) -> None:
    """
    Configure logging for the GIS pipeline tool.

    This sets up rotating file and console handlers.
    Should be called once, ideally at program start.

    Args:
        log_file: Optional log file path.
        level: Optional logging level.
    """
    log_file = log_file or os.path.abspath(config.LOG_FILENAME)
    level = level or getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO)

    logger = logging.getLogger('gis_tool')
    logger.setLevel(level)

    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Ensure log directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

