"""
spatial_utils.py

Utility functions for spatial data validation, reprojection, and geometry operations
in the GIS pipeline.

Includes CRS validation, geometry validation, reprojection of geometries,
and spatial intersection checks.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import warnings

import geopandas as gpd
from pyproj import CRS
from shapely.geometry.base import BaseGeometry

from . import config

logger = logging.getLogger("gis_tool.spatial_utils")


def validate_and_reproject_crs(
    gdf: gpd.GeoDataFrame,
    reference_crs,
    dataset_name: str,
    default_crs: str | None = None,  # e.g. "EPSG:32633"
) -> gpd.GeoDataFrame:
    """
    Ensure the GeoDataFrame shares the same CRS as the reference CRS.
    If not, reproject to the reference CRS.
    If GeoDataFrame has no CRS, optionally assign default_crs.

    Args:
        gdf (GeoDataFrame): GeoDataFrame to validate and reproject.
        reference_crs: Target CRS to match (CRS object, dict, or string).
        dataset_name (str): Dataset name for logging.
        default_crs (str|None): Optional CRS to assign if gdf.crs is missing.

    Returns:
        GeoDataFrame: With CRS matching reference_crs.

    Raises:
        ValueError: If GeoDataFrame has no CRS and default_crs is None.
    """
    if gdf.crs is None:
        if default_crs:
            logger.warning(
                f"[CRS Validation] Dataset '{dataset_name}' has no CRS defined. "
                f"Assigning default CRS '{default_crs}'."
            )
            gdf = gdf.set_crs(default_crs)
        else:
            logger.error(f"[CRS Validation] Dataset '{dataset_name}' has no CRS defined and no default CRS provided.")
            raise ValueError(f"Dataset '{dataset_name}' is missing a CRS.")

    try:
        target_crs = CRS(reference_crs)
        current_crs = CRS(gdf.crs)

        if not current_crs.equals(target_crs):
            logger.warning(
                f"[CRS Validation] Dataset '{dataset_name}' CRS mismatch: "
                f"{current_crs.to_string()} → {target_crs.to_string()}. Auto-reprojecting."
            )
            gdf = gdf.to_crs(target_crs.to_string())
        else:
            logger.info(
                f"[CRS Validation] Dataset '{dataset_name}' already in target CRS ({target_crs.to_string()})."
            )
    except Exception as e:
        logger.error(f"[CRS Validation] Failed to validate or reproject CRS for dataset '{dataset_name}': {e}")
        raise

    return gdf


def validate_geometry_column(
    gdf: gpd.GeoDataFrame,
    dataset_name: str,
    allowed_geom_types=None
) -> gpd.GeoDataFrame:
    """
    Validate GeoDataFrame's 'geometry' column exists, warn on empty geometries,
    and optionally check allowed geometry types.

    Args:
        gdf (GeoDataFrame): GeoDataFrame to validate.
        dataset_name (str): Dataset name for logging.
        allowed_geom_types (list or set, optional): Allowed geometry types.

    Returns:
        GeoDataFrame: Same GeoDataFrame if valid.

    Raises:
        ValueError: If 'geometry' column missing.
    """
    try:
        if "geometry" not in gdf.columns:
            logger.error(f"[Geometry Validation] Dataset '{dataset_name}' missing 'geometry' column.")
            raise ValueError(f"Dataset '{dataset_name}' must have a 'geometry' column.")

        if gdf.geometry.is_empty.any():
            logger.warning(f"[Geometry Validation] Dataset '{dataset_name}' contains empty geometries.")

        if allowed_geom_types is not None:
            invalid_types = gdf.geometry.geom_type[~gdf.geometry.geom_type.isin(allowed_geom_types)]
            if not invalid_types.empty:
                logger.warning(
                    f"[Geometry Validation] Dataset '{dataset_name}' contains unsupported geometry types: "
                    f"{set(invalid_types)}"
                )
    except Exception as e:
        logger.error(f"[Geometry Validation] Error validating geometry column in dataset '{dataset_name}': {e}")
        raise

    return gdf


def validate_geometry_crs(geometry: BaseGeometry, expected_crs: str) -> bool:
    """
    Validate if a shapely geometry matches the expected CRS by wrapping in a GeoSeries.

    Args:
        geometry (BaseGeometry): Shapely geometry object.
        expected_crs (str): Expected CRS string (e.g., 'EPSG:4326').

    Returns:
        bool: True if CRS matches or geometry is empty; False otherwise.
    """
    try:
        if geometry.is_empty:
            return True

        gs = gpd.GeoSeries([geometry], crs=expected_crs)
        return gs.crs.to_string() == expected_crs
    except (AttributeError, ValueError, TypeError) as e:
        logger.error(f"CRS validation error: {e}")
        return False


def reproject_geometry_to_crs(
    geom: BaseGeometry,
    source_crs: str,
    target_crs: str
) -> BaseGeometry:
    """
    Reproject a single shapely geometry from source CRS to target CRS.

    Args:
        geom (BaseGeometry): Geometry to reproject.
        source_crs (str): Current CRS (e.g., 'EPSG:4326').
        target_crs (str): Target CRS (e.g., 'EPSG:3857').

    Returns:
        BaseGeometry: Reprojected geometry.

    Raises:
        Exception: On reprojection failure.
    """
    try:
        geo_series = gpd.GeoSeries([geom], crs=source_crs)
        reprojected_series = geo_series.to_crs(target_crs)
        return reprojected_series.iloc[0]
    except Exception as e:
        logger.error(f"Failed to reproject geometry: {e}")
        raise


def ensure_projected_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Ensure GeoDataFrame has a projected CRS. If not, reproject to config.DEFAULT_CRS.

    Args:
        gdf (GeoDataFrame): Input GeoDataFrame.

    Returns:
        GeoDataFrame: Projected GeoDataFrame.

    Raises:
        ValueError: If input GeoDataFrame has no CRS defined.
    """
    logger.debug(f"ensure_projected_crs called with CRS: {gdf.crs}")

    if gdf.crs is None:
        warnings.warn("Input GeoDataFrame has no CRS defined. Cannot proceed without CRS.", UserWarning)
        raise ValueError("Input GeoDataFrame has no CRS defined.")

    try:
        if not gdf.crs.is_projected:
            warnings.warn(
                f"Input CRS {gdf.crs} is not projected. Reprojecting to {config.DEFAULT_CRS}.",
                UserWarning
            )
            logger.info(f"Reprojecting from {gdf.crs} to {config.DEFAULT_CRS}")
            gdf = gdf.to_crs(config.DEFAULT_CRS)
        else:
            logger.debug("GeoDataFrame already has projected CRS.")
    except Exception as e:
        logger.error(f"Failed to ensure projected CRS: {e}")
        raise

    return gdf


def buffer_intersects_gas_lines(
    buffer_geom: BaseGeometry,
    gas_lines_gdf: gpd.GeoDataFrame
) -> bool:
    """
    Check if buffer geometry intersects any gas line geometries.

    Args:
        buffer_geom (BaseGeometry): Buffer geometry to test.
        gas_lines_gdf (GeoDataFrame): GeoDataFrame of gas lines.

    Returns:
        bool: True if intersection found; False otherwise.
    """
    if buffer_geom is None or buffer_geom.is_empty:
        logger.warning("Empty or None buffer geometry received for intersection check.")
        return False

    try:
        # Use spatial index to find candidates intersecting bounding box of buffer
        possible_matches_index = list(gas_lines_gdf.sindex.intersection(buffer_geom.bounds))
        possible_matches = gas_lines_gdf.iloc[possible_matches_index]

        # Check precise intersection on candidate geometries
        intersects = possible_matches.geometry.intersects(buffer_geom).any()

        logger.debug(f"Buffer intersects gas lines: {intersects}")
        return intersects
    except Exception as e:
        logger.error(f"Error checking intersection between buffer and gas lines: {e}")

        # Fallback: brute force check all gas lines
        for gas_line_geom in gas_lines_gdf.geometry:
            if gas_line_geom.intersects(buffer_geom):
                logger.debug("Buffer intersects gas line found in fallback check.")
                return True

        return False
