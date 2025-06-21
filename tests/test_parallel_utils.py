# tests/test_parallel_utils.py

import logging

from gis_tool.parallel_utils import parallel_process

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Enable DEBUG level to capture detailed logs


def square(x):
    """Simple function to square a number."""
    return x * x


def faulty_func(x):
    """Function that raises an error when x == 2, used to test exception handling."""
    if x == 2:
        raise ValueError("Intentional error for testing")
    return x * 2


def test_parallel_process_returns_expected():
    """
    Test that parallel_process correctly applies a function to a list of inputs
    and returns the expected results in order.
    """
    logger.info("Running test_parallel_process_returns_expected")
    inputs = [1, 2, 3, 4]
    expected = [1, 4, 9, 16]

    results = parallel_process(square, inputs)

    assert results == expected, "parallel_process did not return expected squared results."
    logger.info("test_parallel_process_returns_expected passed.")


def test_parallel_process_empty_input():
    """
    Test that parallel_process handles empty input list gracefully,
    returning an empty list without errors.
    """
    logger.info("Running test_parallel_process_empty_input")
    inputs = []
    results = parallel_process(square, inputs)
    assert results == [], "parallel_process should return empty list for empty input."
    logger.info("test_parallel_process_empty_input passed.")


def test_parallel_process_exception_handling(caplog):
    """
    Test that parallel_process handles exceptions raised by the function gracefully,
    logging errors and returning None for failed items.
    """
    logger.info("Running test_parallel_process_exception_handling")

    inputs = [1, 2, 3]
    with caplog.at_level(logging.ERROR, logger="gis_tool.parallel_utils"):
        results = parallel_process(faulty_func, inputs)

    # The item that caused exception should have None in results
    assert results == [2, None, 6], "parallel_process should return None for failed tasks."
    # Check that an error was logged for the failure
    error_logs = [record for record in caplog.records if record.levelname == "ERROR"]
    assert any("Intentional error for testing" in e.message for e in error_logs), "Error log missing."
    logger.info("test_parallel_process_exception_handling passed.")
