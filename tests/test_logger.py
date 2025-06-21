"""
test_logger.py

Tests for the logging setup in the GIS pipeline tool. Validates that
setup_logging() correctly configures both file and console logging.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import os
from logging.handlers import RotatingFileHandler

from gis_tool.logger import setup_logging


def reset_logging():
    """
    Reset the 'gis_tool' logger to a clean state by removing all handlers.
    Useful for isolating log configuration across multiple tests.
    """
    logger = logging.getLogger("gis_tool.tests")
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)


def test_setup_logging_creates_log_file(tmp_path, capsys):
    """
    Test that setup_logging:
    - Creates the specified log file.
    - Attaches both RotatingFileHandler and StreamHandler to 'gis_tool'.
    - Properly logs messages to file and console.
    """
    os.chdir(tmp_path)
    reset_logging()

    # Define a test log file path inside a temporary directory
    log_file_path = tmp_path / "pipeline_processing.log"
    setup_logging(log_file=str(log_file_path))

    logger = logging.getLogger("gis_tool")
    test_msg = "Test log message"
    logger.info(test_msg)

    # Flush all handlers to make sure message is written
    for handler in logger.handlers:
        handler.flush()

    # Check that log file was created and contains the test message
    assert log_file_path.exists(), "Log file was not created"

    with open(log_file_path, encoding='utf-8') as f:
        log_content = f.read()
    assert test_msg in log_content, "Message not found in log file"

    # Capture console output to verify StreamHandler is working
    captured = capsys.readouterr()
    assert test_msg in captured.out or test_msg in captured.err, "Message not found in console output"

    # Ensure both expected handler types are attached
    handler_types = {type(h) for h in logger.handlers}
    assert RotatingFileHandler in handler_types, "RotatingFileHandler not attached"
    assert logging.StreamHandler in handler_types, "StreamHandler not attached"

    # Verify default logger level is INFO or more verbose
    assert logger.level <= logging.INFO


def test_setup_logging_respects_log_level_env(monkeypatch, capsys):
    """
    Test that setup_logging honors the LOG_LEVEL environment variable
    and applies the correct log level to the logger.
    """
    # Override the LOG_LEVEL environment variable for this test
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    reset_logging()

    # Run setup_logging without manually setting the level
    setup_logging()

    logger = logging.getLogger("gis_tool.tests")
    assert logger.level == logging.DEBUG, "Logger level did not respect LOG_LEVEL env var"

    debug_msg = "Debug message for test"
    logger.debug(debug_msg)

    # Capture the console to verify debug message appears
    captured = capsys.readouterr()
    assert debug_msg in captured.out or debug_msg in captured.err, "Debug message not found in output"
