"""
Utility functions for the GIS pipeline.

Includes robust date parsing, unit conversion, and GeoDataFrame cleaning.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
from typing import Any, Union

import geopandas as gpd
import pandas as pd
from dateutil.parser import parse

logger = logging.getLogger("gis_tool.utils")

# Schema fields for reference. 'Material' field normalized to lowercase for consistency,
# while other string fields like 'Name' keep original casing.
SCHEMA_FIELDS = ["Name", "Date", "PSI", "Material", "geometry"]


def robust_date_parse(date_val: Any) -> Union[pd.Timestamp, pd.NaT]:
    """
    Robustly parse various date formats or objects into a pandas Timestamp.

    Args:
        date_val (Any): Input date value (string, Timestamp, or NaN).

    Returns:
        pd.Timestamp or pd.NaT: Parsed date or NaT if parsing fails.
    """
    logger.debug(f"Parsing date value: {date_val}")
    if pd.isna(date_val):
        logger.debug("Date value is NaN or None; returning pd.NaT.")
        return pd.NaT

    if isinstance(date_val, pd.Timestamp):
        logger.debug("Date value is already a pandas Timestamp.")
        return date_val

    if isinstance(date_val, str):
        # Try common date formats first
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                parsed = pd.to_datetime(date_val, format=fmt)
                logger.debug(f"Date parsed successfully with format {fmt}: {parsed}")
                return parsed
            except (ValueError, TypeError):
                continue

        # Fallback to dateutil parser for more complex strings
        try:
            parsed = pd.to_datetime(parse(date_val, fuzzy=False))
            logger.debug(f"Date parsed successfully with dateutil parser: {parsed}")
            return parsed
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse date string '{date_val}'; returning pd.NaT.")
            return pd.NaT

    logger.warning(f"Unsupported date type ({type(date_val)}) encountered; returning pd.NaT.")
    return pd.NaT


def convert_ft_to_m(feet: float) -> float:
    """
    Convert a distance from feet to meters.

    Args:
        feet (float): Distance in feet.

    Returns:
        float: Distance converted to meters.
    """
    meters = feet * 0.3048
    logger.debug(f"Converted {feet} feet to {meters} meters.")
    return meters


def clean_geodataframe(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Remove rows from a GeoDataFrame that have null, invalid, or empty geometries.

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame.

    Returns:
        gpd.GeoDataFrame: Cleaned GeoDataFrame with only valid, non-empty geometries.
    """
    logger.debug(f"Cleaning GeoDataFrame with {len(gdf)} rows for valid geometries.")
    valid_gdf = gdf[gdf.geometry.notnull() & gdf.geometry.is_valid & ~gdf.geometry.is_empty]
    cleaned_gdf = valid_gdf.reset_index(drop=True)
    logger.debug(f"Cleaned GeoDataFrame has {len(cleaned_gdf)} rows after filtering.")
    return cleaned_gdf
