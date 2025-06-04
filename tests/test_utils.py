### Tests for utils.py
import logging
import pytest
import pandas as pd

from gis_tool.utils import (
    robust_date_parse,
    convert_ft_to_m,
)

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


@pytest.mark.parametrize("input_str, expected", [
    ("2023-05-01", pd.Timestamp("2023-05-01")),
    ("01/05/2023", pd.Timestamp("2023-05-01")),
    ("05/01/2023", pd.Timestamp("2023-01-05")),
    ("not a date", pd.NaT),
    (None, pd.NaT),
])

def test_robust_date_parse(input_str, expected):
    """
     Tests the robust_date_parse function with various date string formats and invalid inputs.
     Verifies that the function correctly parses valid dates or returns pd.NaT for invalid inputs.
     """
    logger.info(f"Testing robust_date_parse with input: {input_str}")
    result = robust_date_parse(input_str)
    if pd.isna(expected):
        assert pd.isna(result)
        logger.debug(f"Input '{input_str}' correctly parsed as NaT.")
    else:
        assert result == expected
        logger.debug(f"Input '{input_str}' correctly parsed as {result}.")


def test_convert_ft_to_m():
    assert convert_ft_to_m(1) == 0.3048
    assert convert_ft_to_m(0) == 0.0
    assert abs(convert_ft_to_m(100) - 30.48) < 1e-6
