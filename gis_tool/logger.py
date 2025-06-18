"""
logger.py

This module sets up consistent logging for the GIS pipeline tool using Python's
standard `logging` module. It configures both a rotating file handler and a
console handler for robust logging during CLI or batch execution.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

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
        log_file (str, optional): Path to log file. Defaults to config.LOG_FILENAME.
        level (int or str, optional): Logging level. Defaults to LOG_LEVEL env var or INFO.
    """
    # Use provided log file or fall back to default from config
    log_file = log_file or os.path.abspath(config.LOG_FILENAME)

    # Determine log level (accepts string or int)
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    else:
        level = level or getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO)

    logger = logging.getLogger('gis_tool')
    logger.setLevel(level)

    # Remove existing handlers to prevent duplicate log entries
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    # Ensure that the log file directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # Set unified log formatting
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Configure rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Configure console handler for stdout logging
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
