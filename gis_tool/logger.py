### logger.py

import logging
import logging.handlers
import os

from gis_tool import config


def setup_logging(log_file: str = None, level: int = None) -> None:
    """
    Configure logging for the GIS pipeline tool.

    Sets up a rotating file handler and a console handler on the 'gis_tool' logger.
    Clears existing handlers before adding new ones to avoid duplicate logs.

    Args:
        log_file: Optional path to the log file. Defaults to config.LOG_FILENAME.
        level: Optional logging level (int or str). Defaults to environment variable LOG_LEVEL or INFO.
    """
    # Determine log file path
    log_file = log_file or os.path.abspath(config.LOG_FILENAME)

    # Determine log level, support string or int input
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    else:
        level = level or getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO)

    logger = logging.getLogger('gis_tool')
    logger.setLevel(level)

    # Clear existing handlers to prevent duplicate logs
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    # Ensure directory for log file exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Setup Rotating File Handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Setup Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
