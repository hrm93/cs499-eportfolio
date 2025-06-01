"""
General utility functions for the GIS pipeline.
"""
import logging
from typing import Any, Dict, Optional, Union

import geopandas as gpd
import pandas as pd
from dateutil.parser import parse
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry
from shapely.errors import TopologicalError

logger = logging.getLogger("gis_tool")

# Note: 'material' field is normalized to lowercase for consistency.
# Other string fields like 'name' retain original casing.
SCHEMA_FIELDS = ["Name", "Date", "PSI", "Material", "geometry"]


def robust_date_parse(date_val: Any) -> Union[pd.Timestamp, pd.NaT]:
    """
    Robustly parse various date formats or objects into a pandas Timestamp.

    Args:
        date_val (Any): Input date value (can be string, Timestamp, or NaN).

    Returns:
        Union[pd.Timestamp, pd.NaT]: A valid pandas Timestamp or pd.NaT if parsing fails.
    """
    logger.debug(f"Parsing date: {date_val}")
    if pd.isna(date_val):
        logger.debug("Date value is NaN or None; returning pd.NaT.")
        return pd.NaT
    if isinstance(date_val, pd.Timestamp):
        logger.debug("Date value is already a pandas Timestamp.")
        return date_val
    if isinstance(date_val, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                parsed = pd.to_datetime(date_val, format=fmt)
                logger.debug(f"Date parsed using format {fmt}: {parsed}")
                return parsed
            except (ValueError, TypeError):
                continue
        try:
            parsed = pd.to_datetime(parse(date_val, fuzzy=False))
            logger.debug(f"Date parsed using dateutil: {parsed}")
            return parsed
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse date: {date_val}; returning pd.NaT.")
            return pd.NaT
    logger.warning(f"Unsupported date type: {type(date_val)}; returning pd.NaT.")
    return pd.NaT


def convert_ft_to_m(feet: float) -> float:
    """
    Convert feet to meters.

    Args:
        feet (float): The distance in feet.

    Returns:
        float: The distance in meters.
    """
    return feet * 0.3048


def clean_geodataframe(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Removes rows with invalid or empty geometries from a GeoDataFrame.

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame.

    Returns:
        gpd.GeoDataFrame: Cleaned GeoDataFrame with only valid, non-empty geometries.
    """
    valid_gdf = gdf[gdf.geometry.notnull() & gdf.geometry.is_valid & ~gdf.geometry.is_empty]
    return valid_gdf.reset_index(drop=True)


def fix_geometry(g: BaseGeometry) -> Optional[BaseGeometry]:
    """
    Fix invalid geometries by applying a zero-width buffer.

    Args:
        g (shapely.geometry.base.BaseGeometry): Geometry to check and fix.

    Returns:
        Geometry or None: Valid geometry or None if it cannot be fixed.

    Notes:
        Buffering with zero-width is a common fix for invalid geometries,
        but may raise Shapely-specific exceptions such as TopologicalError.
        This function catches these explicitly to prevent crashing and logs errors.
        It also catches generic exceptions as a fallback for unexpected errors.
    """
    logger.debug(f"fix_geometry called with geometry: {g}")
    if g is None:
        # Silent skip
        return None
    if g.is_valid:
        logger.debug("Geometry is already valid.")
        return g
    try:
        fixed = g.buffer(0)
        if fixed.is_empty or not fixed.is_valid:
            logger.warning("Geometry could not be fixed (empty or invalid after buffering).")
            return None
        logger.debug("Geometry fixed using zero-width buffer.")
        return fixed
    except TopologicalError as exc:
        logger.error(f"Topological error fixing geometry: {exc}")
        return None
    except Exception as exc:
        logger.error(f"Unexpected error fixing geometry: {exc}")
        return None


def simplify_geometry(geom: BaseGeometry, tolerance: float = 0.00001) -> Dict:
    """
    Simplify a geometry to reduce floating point precision issues.

    Uses Shapely's simplify method with topology preservation to reduce
    the complexity of the geometry while maintaining its shape.

    Args:
        geom (BaseGeometry): The input Shapely geometry to simplify.
        tolerance (float, optional): The tolerance threshold for simplification.
            Defaults to 0.00001.

    Returns:
        dict: A GeoJSON-like mapping dictionary of the simplified geometry.
    """
    # Simplify geometry to avoid floating point precision issues
    logger.debug(f"simplify_geometry called with tolerance: {tolerance} for geometry type: {geom.geom_type}")
    simplified = geom.simplify(tolerance, preserve_topology=True)
    logger.debug("Geometry simplified.")
    return mapping(simplified)