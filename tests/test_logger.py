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

    Args:
        tmp_path (pathlib.Path): Temporary directory provided by pytest for isolated file testing.
        capsys: Pytest fixture to capture stdout and stderr output.
    """
    # Change working directory to temporary directory to isolate log file creation
    os.chdir(tmp_path)
    reset_logging()  # Clean any existing logging config before setup

    logger = setup_logging()

    log_file = tmp_path / "pipeline_processing.log"
    logger.info("Test log message")

    # Verify the log file was created
    assert log_file.exists(), "Log file was not created"

    # Capture console output and verify the test message is present
    captured = capsys.readouterr()
    assert ("Test log message" in captured.err) or ("Test log message" in captured.out)

    # Confirm the logger has the correct name
    assert logger.name == 'gis_tool'

    # Confirm both RotatingFileHandler and StreamHandler are attached to the logger
    handler_types = {type(h) for h in logger.handlers}
    assert RotatingFileHandler in handler_types, "RotatingFileHandler not attached to logger"
    assert logging.StreamHandler in handler_types, "StreamHandler not attached to logger"

    # Logger level should be INFO or more verbose by default
    assert logger.level <= logging.INFO

def test_setup_logging_respects_log_level_env(monkeypatch, capsys):
    """
    Test that setup_logging respects the LOG_LEVEL environment variable
    to configure the logger's level accordingly.

    Args:
        monkeypatch: Pytest fixture to temporarily set environment variables.
        capsys: Pytest fixture to capture stdout and stderr output.
    """
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    reset_logging()
    logger = setup_logging()

    # Verify that the logger level is set to DEBUG as per environment variable
    assert logger.level == logging.DEBUG

    # Log a debug message and verify it appears in console output
    logger.debug("Debug message for test")
    captured = capsys.readouterr()
    assert ("Debug message for test" in captured.err) or ("Debug message for test" in captured.out)
