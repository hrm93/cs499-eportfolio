### Tests for geometry_cleaning.py

import logging
import geopandas as gpd

from unittest.mock import Mock
from shapely.geometry import LineString, Point, Polygon, mapping
from shapely.geometry.base import BaseGeometry

from gis_tool.geometry_cleaning import fix_geometry, simplify_geometry

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


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


def test_geometry_simplification_accuracy():
    """
    Test that simplifying buffered geometries preserves topology and does not lose data integrity.
    """
    poly = Point(0, 0).buffer(10)  # Big buffer
    gdf = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:4326")
    simplified = gdf.geometry.simplify(1.0)  # Simplify with tolerance

    # Check that simplified geometry still overlaps original
    assert simplified[0].intersects(poly), "Simplification should preserve spatial overlap"


def test_simplify_geometry_returns_mapping():
    """
    Tests simplify_geometry returns a GeoJSON-like dict with geometry simplified
    according to the specified tolerance, coordinates within tolerance,
    and correct geometry type.
    """
    logger.info("Testing simplify_geometry function.")

    # Test with Point
    point = Point(10.123456789, 20.987654321)
    simplified_geom = simplify_geometry(point, tolerance=0.01)
    simplified_point = mapping(simplified_geom)  # convert to dict

    assert isinstance(simplified_point, dict)
    assert simplified_point.get('type') == 'Point'
    coords = simplified_point.get('coordinates', [])
    assert abs(coords[0] - 10.123456789) < 0.01
    assert abs(coords[1] - 20.987654321) < 0.01

    # Test with Polygon
    polygon = Polygon([(0, 0), (1, 1), (1, 0)])
    simplified_poly = mapping(simplify_geometry(polygon, tolerance=0.1))
    assert isinstance(simplified_poly, dict)
    assert simplified_poly.get('type') == 'Polygon'
    assert 'coordinates' in simplified_poly

    # Test with LineString
    line = LineString([(0, 0), (1, 1), (2, 2)])
    simplified_line = mapping(simplify_geometry(line, tolerance=0.1))
    assert isinstance(simplified_line, dict)
    assert simplified_line.get('type') == 'LineString'
    assert 'coordinates' in simplified_line

    logger.info("simplify_geometry test passed.")
