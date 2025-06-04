"""
General utility functions for the GIS pipeline.
"""
import logging
from typing import Any, Union

import geopandas as gpd
import pandas as pd
from dateutil.parser import parse

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
