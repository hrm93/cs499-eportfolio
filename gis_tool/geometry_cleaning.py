"""
geometry_cleaning.py

This module provides utility functions for cleaning, fixing, and validating
Shapely geometries in geospatial workflows. It includes automatic CRS
detection, simplification of geometries, geometry validation via buffering,
and checks for finite coordinate values.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import math
from typing import Optional, Dict, Any

import geopandas as gpd
from pyproj import CRS
from shapely.errors import TopologicalError
from shapely.geometry.base import BaseGeometry

logger = logging.getLogger("gis_tool.geometry_cleaning")


def get_utm_crs_for_gdf(gdf: gpd.GeoDataFrame) -> CRS:
    """
    Determine an appropriate UTM CRS based on the centroid of a GeoDataFrame.

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame with geometries.

    Returns:
        CRS: A pyproj CRS object representing the estimated UTM zone.
    """
    centroid = gdf.unary_union.centroid
    utm_zone = int((centroid.x + 180) / 6) + 1
    is_northern = centroid.y >= 0
    crs_code = 32600 + utm_zone if is_northern else 32700 + utm_zone
    logger.debug(f"Calculated UTM zone: {utm_zone}, Northern Hemisphere: {is_northern}, EPSG: {crs_code}")
    return CRS.from_epsg(crs_code)


def fix_geometry(g: BaseGeometry) -> Optional[BaseGeometry]:
    """
    Attempt to fix invalid Shapely geometries by applying a zero-width buffer.

    Args:
        g (BaseGeometry): The geometry to be checked and fixed.

    Returns:
        Optional[BaseGeometry]: The fixed geometry, or None if it cannot be fixed.
    """
    logger.debug(f"fix_geometry called with geometry: {g}")
    if g is None:
        return None
    if g.is_valid:
        logger.debug("Geometry is already valid.")
        return g
    try:
        fixed = g.buffer(0)
        if fixed.is_empty or not fixed.is_valid:
            logger.warning("Geometry could not be fixed (empty or still invalid after buffering).")
            return None
        logger.debug("Geometry fixed using zero-width buffer.")
        return fixed
    except TopologicalError as exc:
        logger.error(f"Topological error while fixing geometry: {exc}")
        return None
    except Exception as exc:
        logger.error(f"Unexpected error while fixing geometry: {exc}")
        return None


def simplify_geometry(geom: BaseGeometry, tolerance: float = 0.00001) -> BaseGeometry:
    """
    Simplify geometry to reduce complexity while preserving topology.

    Args:
        geom (BaseGeometry): The Shapely geometry to simplify.
        tolerance (float): Tolerance value for simplification.

    Returns:
        BaseGeometry: A simplified geometry.
    """
    logger.debug(f"simplify_geometry called with tolerance={tolerance} for geometry type: {geom.geom_type}")
    simplified = geom.simplify(tolerance, preserve_topology=True)
    logger.debug("Geometry simplification complete.")
    return simplified


def is_finite_geometry(geom: Optional[Dict[str, Any]]) -> bool:
    """
    Recursively check if all coordinates in a GeoJSON geometry are finite numbers.

    Args:
        geom (Optional[Dict[str, Any]]): A geometry dictionary in GeoJSON format.

    Returns:
        bool: True if all coordinate values are finite, False otherwise.
    """
    if not geom or "coordinates" not in geom:
        logger.debug("Geometry is None or missing 'coordinates'.")
        return False

    coords = geom["coordinates"]
    if not isinstance(coords, (list, tuple)):
        logger.warning("Geometry coordinates are not list or tuple.")
        return False

    def check_finite_coords(coords_part):
        """Helper to recursively check coordinate values."""
        if isinstance(coords_part, (list, tuple)):
            return all(check_finite_coords(item) for item in coords_part)
        elif isinstance(coords_part, (int, float)):
            return math.isfinite(coords_part)
        else:
            return False

    finite_check = check_finite_coords(coords)
    if not finite_check:
        logger.warning(f"Non-finite geometry detected (type: {geom.get('type')}): {geom}")
    return finite_check
