### logger.py

import logging
import logging.handlers
import os

from gis_tool import config  # Assuming config.py defines LOG_FILENAME and possibly LOG_LEVEL

def setup_logging(log_file: str = None, level: int = None) -> logging.Logger:
    """
    Configure and return a logger for the GIS pipeline tool.

    This logger writes messages to both a rotating log file and the console.
    It includes detailed information such as timestamp, log level, logger name,
    function name, and line number for easier debugging and traceability.

    The log file rotates when it reaches 5 MB, keeping up to 3 backup files to
    prevent unbounded growth.

    Args:
        log_file (str, optional): Path to the log file. Defaults to the path defined
            in the configuration module (`config.LOG_FILENAME`).
        level (int, optional): Logging level (e.g., logging.INFO, logging.DEBUG).
            Defaults to the environment variable 'LOG_LEVEL' if set, otherwise INFO.

    Returns:
        logging.Logger: Configured logger instance ready for use.

    Example:
        >> logger = setup_logging()
        >> logger.info("GIS pipeline started.")
    """
    log_file = log_file or os.path.abspath(config.LOG_FILENAME)
    level = level or getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO)

    logger = logging.getLogger('gis_tool')
    logger.setLevel(level)

    # Remove any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Rotating file handler: max 5 MB per file, keep 3 backups
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
