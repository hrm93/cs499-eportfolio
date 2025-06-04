### Tests for parallel_utils

import logging

from gis_tool.parallel_utils import parallel_process

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


def square(x):
    return x * x

def test_parallel_process_returns_expected():
    """
    Test parallel_process runs the function on a list of inputs and returns expected results.
    """
    logger.info("Running test_parallel_process_returns_expected")
    inputs = [1, 2, 3, 4]
    expected = [1, 4, 9, 16]
    results = parallel_process(square, inputs)
    assert results == expected
    logger.info("test_parallel_process_returns_expected passed.")
