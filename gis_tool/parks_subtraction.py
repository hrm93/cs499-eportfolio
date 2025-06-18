"""
parks_subtraction.py

Handles subtraction of park polygons from buffered gas line polygons.
Supports optional multiprocessing and includes geometry validation/fixing.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import warnings
from typing import Optional, Union

from pyproj import CRS
import geopandas as gpd

from .spatial_utils import validate_and_reproject_crs
from gis_tool.geometry_cleaning import fix_geometry
from gis_tool.parallel_utils import parallel_process
from gis_tool.buffer_utils import (
    subtract_park_from_geom_helper,
    subtract_park_from_geom,
    log_and_filter_invalid_geometries,
)

# Set up logger for this module
logger = logging.getLogger("gis_tool.parks_subtraction")


def subtract_parks_from_buffer(
    buffer_gdf: gpd.GeoDataFrame,
    parks_path: Optional[str] = None,
    use_multiprocessing: bool = False,
) -> gpd.GeoDataFrame:
    """
    Subtract park polygons from buffer polygons.

    Args:
        buffer_gdf (gpd.GeoDataFrame): GeoDataFrame of buffered gas lines (polygons).
        parks_path (Optional[str]): File path to park polygons layer.
        use_multiprocessing (bool): If True, use multiprocessing to subtract parks.

    Returns:
        gpd.GeoDataFrame: Updated GeoDataFrame with parks subtracted from buffers.
    """
    logger.info(f"subtract_parks_from_buffer called with parks_path: {parks_path}")

    try:
        # Return original buffer if no parks path provided
        if parks_path is None:
            logger.info("No parks path provided, returning buffer unchanged.")
            return buffer_gdf.copy()

        # Load park geometries
        parks_gdf = gpd.read_file(parks_path)
        logger.debug("Parks layer loaded successfully.")

        # Reproject parks to match buffer CRS
        parks_gdf = validate_and_reproject_crs(parks_gdf, buffer_gdf.crs, "parks")

        # Filter to allowed polygon types only
        allowed_park_types = ['Polygon', 'MultiPolygon']
        invalid_park_types = parks_gdf.geom_type[~parks_gdf.geom_type.isin(allowed_park_types)]
        if not invalid_park_types.empty:
            logger.warning(
                f"Unsupported geometry types in parks layer: {invalid_park_types.unique()}. These will be excluded."
            )
            parks_gdf = parks_gdf[parks_gdf.geom_type.isin(allowed_park_types)]

        # Fix and validate geometries
        parks_gdf = parks_gdf[parks_gdf.geometry.notnull()]
        parks_gdf['geometry'] = parks_gdf.geometry.apply(fix_geometry)
        parks_gdf = log_and_filter_invalid_geometries(parks_gdf, "Parks")

        buffer_gdf = buffer_gdf[buffer_gdf.geometry.notnull()]
        buffer_gdf['geometry'] = buffer_gdf.geometry.apply(fix_geometry)
        buffer_gdf = log_and_filter_invalid_geometries(buffer_gdf, "Buffers")

        if parks_gdf.empty:
            warnings.warn("No valid park geometries found for subtraction.", UserWarning)
            logger.warning("No valid park geometries found for subtraction.")

        parks_geoms = list(parks_gdf.geometry)
        logger.debug(f"Number of valid park geometries: {len(parks_geoms)}")

        # Subtract parks using multiprocessing or sequential logic
        if use_multiprocessing:
            logger.info("Subtracting parks using multiprocessing.")
            args = [(geom, parks_geoms) for geom in buffer_gdf.geometry]
            buffer_gdf['geometry'] = parallel_process(subtract_park_from_geom_helper, args)
        else:
            logger.info("Subtracting parks sequentially.")
            buffer_gdf['geometry'] = buffer_gdf.geometry.apply(
                lambda geom: subtract_park_from_geom(geom, parks_geoms)
            )

        # Final cleanup of geometry
        buffer_gdf['geometry'] = buffer_gdf.geometry.apply(fix_geometry)
        buffer_gdf = buffer_gdf[buffer_gdf.geometry.is_valid & ~buffer_gdf.geometry.is_empty]

        logger.info(f"Subtraction complete. Remaining features: {len(buffer_gdf)}")
        return buffer_gdf

    except Exception as e:
        logger.exception(f"Error in subtract_parks_from_buffer: {e}")
        warnings.warn(f"Error subtracting parks: {e}", UserWarning)
        raise


# Define accepted CRS input types
CRSLike = Union[str, CRS]
