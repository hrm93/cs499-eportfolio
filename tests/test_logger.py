### test_logger.py

import logging
import os
from gis_tool.logger import setup_logging
from logging.handlers import RotatingFileHandler


def reset_logging():
    """
    Reset the 'gis_tool' logger by removing all handlers and shutting down logging.

    This ensures that each test starts with a clean logging state,
    avoiding interference from previously added handlers.
    """
    logger = logging.getLogger('gis_tool')
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
    logging.shutdown()


def test_setup_logging_creates_log_file(tmp_path, capsys):
    """
    Test that setup_logging:
    - Creates a rotating log file named 'pipeline_processing.log' in the current working directory.
    - Configures logging with RotatingFileHandler and StreamHandler on 'gis_tool' logger.
    - Outputs log messages to both the log file and the console.
    """
    os.chdir(tmp_path)
    reset_logging()
    setup_logging()  # <- doesn't return anything, sets up logging globally

    logger = logging.getLogger("gis_tool")
    logger.info("Test log message")

    log_file = tmp_path / "pipeline_processing.log"
    assert log_file.exists(), "Log file was not created"

    # Capture console output and verify the message appears
    captured = capsys.readouterr()
    assert "Test log message" in captured.out or "Test log message" in captured.err

    # Confirm both handler types are present
    handler_types = {type(h) for h in logger.handlers}
    assert RotatingFileHandler in handler_types, "RotatingFileHandler not attached to logger"
    assert logging.StreamHandler in handler_types, "StreamHandler not attached to logger"

    # Logger level should default to INFO or more verbose
    assert logger.level <= logging.INFO


def test_setup_logging_respects_log_level_env(monkeypatch, capsys):
    """
    Test that setup_logging respects the LOG_LEVEL environment variable
    to configure the logger's level accordingly.
    """
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    reset_logging()
    setup_logging()

    logger = logging.getLogger("gis_tool")
    assert logger.level == logging.DEBUG

    logger.debug("Debug message for test")
    captured = capsys.readouterr()
    assert "Debug message for test" in captured.out or "Debug message for test" in captured.err
