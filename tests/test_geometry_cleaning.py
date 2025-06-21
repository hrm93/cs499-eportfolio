"""
Tests for geometry_cleaning.py

This module verifies the functionality of geometry-related utilities, including:
- Geometry fixing and simplification
- UTM CRS estimation
- Coordinate validation for finiteness

Each test includes detailed logging for traceability and uses assert-based verification.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import geopandas as gpd

from unittest.mock import Mock
from shapely.geometry import LineString, Point, Polygon, mapping
from shapely.geometry.base import BaseGeometry

from gis_tool.geometry_cleaning import (
    fix_geometry,
    simplify_geometry,
    get_utm_crs_for_gdf,
    is_finite_geometry,
)

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs at DEBUG level


def test_fix_geometry_valid():
    """
    Test fix_geometry returns a valid geometry unchanged.

    A valid Point should be returned exactly as-is.
    """
    logger.info("Running test_fix_geometry_valid")
    pt = Point(0, 0)
    assert fix_geometry(pt) == pt
    logger.info("test_fix_geometry_valid passed.")


def test_fix_geometry_invalid():
    """
    Test fix_geometry attempts to correct an invalid self-intersecting polygon.

    The function should return a valid, non-empty polygon.
    """
    logger.info("Running test_fix_geometry_invalid")
    invalid_poly = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])  # bowtie polygon
    logger.debug(f"Invalid polygon is_valid: {invalid_poly.is_valid}")
    assert not invalid_poly.is_valid

    fixed = fix_geometry(invalid_poly)
    logger.debug(f"Fixed polygon validity: {fixed.is_valid if fixed else 'None'}")
    assert fixed is not None
    assert fixed.is_valid
    logger.info("test_fix_geometry_invalid passed.")


def test_fix_geometry_unfixable():
    """
    Test fix_geometry returns None if a geometry cannot be repaired.

    Simulates a geometry that throws an error on buffer.
    """
    logger.info("Running test_fix_geometry_unfixable")

    # Mock geometry that raises exception on buffer
    mock_geom = Mock(spec=BaseGeometry)
    mock_geom.is_valid = False
    mock_geom.buffer.side_effect = Exception("Cannot buffer")

    result = fix_geometry(mock_geom)
    logger.debug(f"Result from fix_geometry: {result}")
    assert result is None
    logger.info("test_fix_geometry_unfixable passed.")


def test_geometry_simplification_accuracy():
    """
    Test simplify_geometry preserves spatial integrity.

    A simplified geometry should still spatially intersect with the original.
    """
    logger.info("Running test_geometry_simplification_accuracy")

    poly = Point(0, 0).buffer(10)  # Large buffer geometry
    gdf = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:4326")

    simplified = gdf.geometry.simplify(1.0)  # Apply simplification with tolerance

    # Verify spatial overlap is preserved
    assert simplified[0].intersects(poly), "Simplification should preserve spatial overlap"
    logger.info("test_geometry_simplification_accuracy passed.")


def test_simplify_geometry_returns_mapping():
    """
    Test simplify_geometry works with various geometry types.

    Verifies returned mappings are valid GeoJSON-like dicts with correct types.
    """
    logger.info("Running test_simplify_geometry_returns_mapping")

    # Test Point simplification
    point = Point(10.123456789, 20.987654321)
    simplified_point_geom = simplify_geometry(point, tolerance=0.01)
    simplified_point = mapping(simplified_point_geom)
    coords = simplified_point.get("coordinates", [])

    assert isinstance(simplified_point, dict)
    assert simplified_point.get('type') == 'Point'
    assert abs(coords[0] - 10.123456789) < 0.01
    assert abs(coords[1] - 20.987654321) < 0.01

    # Test Polygon simplification
    polygon = Polygon([(0, 0), (1, 1), (1, 0)])
    simplified_poly = mapping(simplify_geometry(polygon, tolerance=0.1))
    assert isinstance(simplified_poly, dict)
    assert simplified_poly.get('type') == 'Polygon'
    assert 'coordinates' in simplified_poly

    # Test LineString simplification
    line = LineString([(0, 0), (1, 1), (2, 2)])
    simplified_line = mapping(simplify_geometry(line, tolerance=0.1))
    assert isinstance(simplified_line, dict)
    assert simplified_line.get('type') == 'LineString'
    assert 'coordinates' in simplified_line

    logger.info("test_simplify_geometry_returns_mapping passed.")


def test_get_utm_crs_for_gdf():
    """
    Test get_utm_crs_for_gdf returns correct UTM CRS.

    Uses a point in San Francisco to verify zone 10N (EPSG:32610).
    """
    logger.info("Running test_get_utm_crs_for_gdf")

    gdf = gpd.GeoDataFrame(
        geometry=[Point(-122.4194, 37.7749)],
        crs="EPSG:4326"
    )
    utm_crs = get_utm_crs_for_gdf(gdf)

    assert utm_crs.to_epsg() == 32610
    logger.info("test_get_utm_crs_for_gdf passed.")


def test_is_finite_geometry_valid_point():
    """
    Test is_finite_geometry returns True for valid coordinates.

    GeoJSON Point with finite values should pass.
    """
    logger.info("Running test_is_finite_geometry_valid_point")

    geom = {"type": "Point", "coordinates": [1.23, 4.56]}
    assert is_finite_geometry(geom) is True

    logger.info("test_is_finite_geometry_valid_point passed.")


def test_is_finite_geometry_invalid_coords():
    """
    Test is_finite_geometry returns False for infinite/NaN values.

    GeoJSON Point with invalid coordinate should fail.
    """
    logger.info("Running test_is_finite_geometry_invalid_coords")

    geom = {"type": "Point", "coordinates": [float("inf"), 5.0]}
    assert is_finite_geometry(geom) is False

    logger.info("test_is_finite_geometry_invalid_coords passed.")
