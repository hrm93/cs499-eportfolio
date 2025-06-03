### test_logger.py
import logging
import os
import pytest
from logging.handlers import RotatingFileHandler

from gis_tool.logger import setup_logging


@pytest.fixture(scope="session", autouse=True)
def init_logger():
    """Initialize logging once per test session."""
    setup_logging()


def reset_logging():
    """Remove all handlers from 'gis_tool' logger to reset its state."""
    logger = logging.getLogger('gis_tool')
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)


def test_setup_logging_creates_log_file(tmp_path, capsys):
    """
    Verify that setup_logging:
    - Creates the log file in the specified directory.
    - Attaches both RotatingFileHandler and StreamHandler.
    - Logs output to both file and console.
    """
    os.chdir(tmp_path)
    reset_logging()

    # Explicitly specify log file inside tmp_path for test isolation
    log_file_path = tmp_path / "pipeline_processing.log"
    setup_logging(log_file=str(log_file_path))

    logger = logging.getLogger("gis_tool")
    test_msg = "Test log message"
    logger.info(test_msg)

    # Flush handlers to ensure logs are written
    for handler in logger.handlers:
        handler.flush()

    assert log_file_path.exists(), "Log file was not created"

    # Read the log file content to confirm message was logged
    with open(log_file_path, encoding='utf-8') as f:
        log_content = f.read()
    assert test_msg in log_content

    # Capture console output and verify message presence
    captured = capsys.readouterr()
    assert test_msg in captured.out or test_msg in captured.err

    # Check for correct handler types attached to logger
    handler_types = {type(h) for h in logger.handlers}
    assert RotatingFileHandler in handler_types, "RotatingFileHandler not attached"
    assert logging.StreamHandler in handler_types, "StreamHandler not attached"

    # Logger level should default to INFO or more verbose
    assert logger.level <= logging.INFO


def test_setup_logging_respects_log_level_env(monkeypatch, capsys):
    """
    Verify that setup_logging respects the LOG_LEVEL env variable and sets logger level.
    """
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    reset_logging()
    setup_logging()

    logger = logging.getLogger("gis_tool")
    assert logger.level == logging.DEBUG

    debug_msg = "Debug message for test"
    logger.debug(debug_msg)

    captured = capsys.readouterr()
    assert debug_msg in captured.out or debug_msg in captured.err
