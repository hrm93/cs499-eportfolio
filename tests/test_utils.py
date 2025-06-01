### Tests for utils.py
import logging
from unittest.mock import Mock
import pytest
import pandas as pd

from shapely.geometry import LineString, Point, Polygon
from shapely.geometry.base import BaseGeometry

from gis_tool.utils import (
    robust_date_parse,
    convert_ft_to_m,
    clean_geodataframe,
    fix_geometry,
    simplify_geometry,
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


def test_fix_geometry_valid():
    """
    Test that a valid geometry is returned unchanged by fix_geometry.
    """
    logger.info("Running test_fix_geometry_valid")
    pt = Point(0, 0)
    assert fix_geometry(pt) == pt
    logger.info("test_fix_geometry_valid passed.")


def test_fix_geometry_invalid():
    """
    Test that fix_geometry attempts to fix an invalid self-intersecting polygon.
    The fixed geometry should be valid and not None.
    """
    logger.info("Running test_fix_geometry_invalid")
    invalid_poly = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
    logger.debug(f"Invalid polygon is_valid: {invalid_poly.is_valid}")
    assert not invalid_poly.is_valid

    fixed = fix_geometry(invalid_poly)
    logger.debug(f"Fixed polygon validity: {fixed.is_valid if fixed else 'None'}")
    assert fixed is not None
    assert fixed.is_valid
    logger.info("test_fix_geometry_invalid passed.")


def test_fix_geometry_unfixable():
    """
    Test that fix_geometry returns None if buffering to fix geometry
    raises an exception (simulates unfixable geometry).
    """
    logger.info("Running test_fix_geometry_unfixable")
    mock_geom = Mock(spec=BaseGeometry)
    mock_geom.is_valid = False
    mock_geom.buffer.side_effect = Exception("Cannot buffer")

    result = fix_geometry(mock_geom)
    logger.debug(f"Result from fix_geometry: {result}")
    assert result is None
    logger.info("test_fix_geometry_unfixable passed.")


def test_simplify_geometry_returns_mapping():
    """
    Tests simplify_geometry returns a GeoJSON-like dict with geometry simplified
    according to the specified tolerance.
    """
    logger.info("Testing simplify_geometry function.")

    # Test with Point
    point = Point(10.123456789, 20.987654321)
    simplified_point = simplify_geometry(point, tolerance=0.01)
    logger.debug(f"Simplified Point geometry output: {simplified_point}")
    assert isinstance(simplified_point, dict)
    assert 'type' in simplified_point and simplified_point['type'] == 'Point'
    coords = simplified_point.get('coordinates', [])
    assert abs(coords[0] - 10.123456789) < 0.01
    assert abs(coords[1] - 20.987654321) < 0.01

    # Test with Polygon
    polygon = Polygon([(0, 0), (1, 1), (1, 0)])
    simplified_poly = simplify_geometry(polygon, tolerance=0.1)
    logger.debug(f"Simplified Polygon geometry output: {simplified_poly}")
    assert isinstance(simplified_poly, dict)
    assert 'type' in simplified_poly and simplified_poly['type'] == 'Polygon'
    assert 'coordinates' in simplified_poly

    # Test with LineString
    line = LineString([(0, 0), (1, 1), (2, 2)])
    simplified_line = simplify_geometry(line, tolerance=0.1)
    logger.debug(f"Simplified LineString geometry output: {simplified_line}")
    assert isinstance(simplified_line, dict)
    assert 'type' in simplified_line and simplified_line['type'] == 'LineString'
    assert 'coordinates' in simplified_line

    logger.info("simplify_geometry test passed.")
