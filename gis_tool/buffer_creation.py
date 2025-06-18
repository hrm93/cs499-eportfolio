"""
Module: buffer_creation

Provides functionality to create buffer polygons around gas line features using GeoPandas,
with optional subtraction of park areas and support for parallel processing.

The main function `create_buffer_with_geopandas` reads input gas line data, validates
and buffers geometries, fixes and filters invalid geometries, optionally subtracts parks,
and returns a cleaned GeoDataFrame of buffered polygons.

Dependencies:
- geopandas
- logging
- warnings
- typing (Optional)
- Internal modules for spatial utilities, geometry cleaning, buffering helpers,
  multiprocessing support, parks subtraction, and configuration.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import warnings
from typing import Optional

import geopandas as gpd

from . import spatial_utils
from .spatial_utils import buffer_intersects_gas_lines
from gis_tool import config
from gis_tool.utils import convert_ft_to_m, clean_geodataframe
from gis_tool.geometry_cleaning import fix_geometry
from gis_tool.parallel_utils import parallel_process
from gis_tool.parks_subtraction import subtract_parks_from_buffer
from gis_tool.buffer_utils import (
    buffer_geometry_helper,
    log_and_filter_invalid_geometries,
)

logger = logging.getLogger("gis_tool.buffer_creation")


def create_buffer_with_geopandas(
    input_gas_lines_path: str,
    buffer_distance_ft: Optional[float] = None,
    parks_path: Optional[str] = None,
    use_multiprocessing: bool = False,
) -> gpd.GeoDataFrame:
    """
    Create a buffer polygon around gas line features using GeoPandas.

    This function performs several steps:
    - Reads the input gas line vector data.
    - Assigns CRS if missing and ensures projected CRS for accurate buffering.
    - Validates geometry types and filters unsupported or invalid geometries.
    - Buffers geometries by a specified distance (in feet, converted to meters).
    - Fixes any geometry issues post-buffering.
    - Optionally subtracts park areas from the buffered polygons.
    - Filters final geometries to ensure validity and intersection with original features.

    Args:
        input_gas_lines_path (str): File path to input gas lines layer (shapefile, GeoPackage, etc.).
        buffer_distance_ft (float, optional): Buffer distance in feet. Defaults to config.DEFAULT_BUFFER_DISTANCE_FT.
        parks_path (str, optional): File path to park polygons to subtract from buffer.
        use_multiprocessing (bool, optional): Whether to use parallel processing to speed up buffering and subtraction.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing the buffered (and optionally parks-subtracted) geometries.

    Raises:
        Exception: Propagates exceptions encountered during processing after logging and warning.

    Notes:
        - Parallel processing can improve performance on large or complex datasets but may add overhead.
        - CRS projection is crucial for accurate buffering distances.
        - Filtering ensures only valid and meaningful geometries are returned.
    """
    logger.info(f"create_buffer_with_geopandas called with input: {input_gas_lines_path}")

    if buffer_distance_ft is None:
        buffer_distance_ft = config.DEFAULT_BUFFER_DISTANCE_FT
    buffer_distance_m = convert_ft_to_m(buffer_distance_ft)
    logger.debug(f"Buffer distance in meters: {buffer_distance_m}")

    try:
        # Load gas lines data
        gas_lines_gdf = gpd.read_file(input_gas_lines_path)
        logger.debug("Gas lines layer loaded.")

        # Assign default CRS if missing
        if gas_lines_gdf.crs is None:
            warnings.warn("Input gas lines layer has no CRS. Assigning default CRS.", UserWarning)
            logger.warning("Input gas lines layer has no CRS; assigning default.")
            gas_lines_gdf = gas_lines_gdf.set_crs(config.DEFAULT_CRS)

        # Ensure projected CRS for accurate buffering
        gas_lines_gdf = spatial_utils.ensure_projected_crs(gas_lines_gdf)

        # === VALIDATION CHECKS BEFORE BUFFERING ===

        # Filter unsupported geometry types (keep only point and line geometries)
        allowed_geom_types = ['Point', 'LineString', 'MultiLineString', 'MultiPoint']
        invalid_geom_types = gas_lines_gdf.geom_type[~gas_lines_gdf.geom_type.isin(allowed_geom_types)]
        if not invalid_geom_types.empty:
            logger.warning(
                f"Unsupported geometry types found in gas lines for buffering: {invalid_geom_types.unique()}. "
                "These features will be excluded from buffering."
            )
            gas_lines_gdf = gas_lines_gdf[gas_lines_gdf.geom_type.isin(allowed_geom_types)]

        # Log and filter invalid geometries
        gas_lines_gdf = log_and_filter_invalid_geometries(gas_lines_gdf, "Gas Lines")

        # Early return if no valid geometries
        if gas_lines_gdf.empty:
            warnings.warn("No valid gas line geometries found for buffering after validation.", UserWarning)
            logger.warning("Gas lines GeoDataFrame is empty after filtering invalid geometries.")
            return gas_lines_gdf

        # === BUFFERING ===
        if use_multiprocessing:
            logger.info("Buffering geometries with multiprocessing.")
            args = [(geom, buffer_distance_m) for geom in gas_lines_gdf.geometry]
            gas_lines_gdf['geometry'] = parallel_process(buffer_geometry_helper, args)
        else:
            logger.info("Buffering geometries sequentially.")
            gas_lines_gdf['geometry'] = gas_lines_gdf.geometry.buffer(buffer_distance_m)

        # Fix geometries after buffering to ensure validity
        gas_lines_gdf['geometry'] = gas_lines_gdf.geometry.apply(fix_geometry)

        # Log and filter invalid geometries after buffering
        gas_lines_gdf = log_and_filter_invalid_geometries(gas_lines_gdf, "Buffered Gas Lines")

        # Early return if buffering removed all valid geometries
        if gas_lines_gdf.empty:
            warnings.warn("No valid buffer geometries remain after buffering.", UserWarning)
            logger.warning("Buffered gas lines GeoDataFrame is empty after filtering invalid geometries.")
            return gas_lines_gdf

        # Subtract parks polygons from buffers if parks_path provided
        if parks_path:
            warnings.warn("Subtracting parks from buffers. Ensure parks data is clean and valid.", UserWarning)
            logger.info(f"Subtracting parks from buffers using parks layer at {parks_path}")
            gas_lines_gdf = subtract_parks_from_buffer(gas_lines_gdf, parks_path)

        logger.debug("Buffering complete.")

        # Filter buffered polygons to those intersecting the original gas lines
        gas_lines_gdf = gas_lines_gdf[
            gas_lines_gdf.geometry.apply(lambda buf_geom: buffer_intersects_gas_lines(buf_geom, gas_lines_gdf))
        ]

        # Clean and finalize GeoDataFrame before returning
        gas_lines_gdf = clean_geodataframe(gas_lines_gdf)

        # Final validation: keep only valid, non-empty geometries
        gas_lines_gdf = gas_lines_gdf[gas_lines_gdf.geometry.is_valid & ~gas_lines_gdf.geometry.is_empty]

        logger.info(f"Final GeoDataFrame contains {len(gas_lines_gdf)} valid, non-empty geometries.")
        return gas_lines_gdf

    except Exception as e:
        logger.exception(f"Error in create_buffer_with_geopandas: {e}")
        warnings.warn(f"Error creating buffer: {e}", UserWarning)
        raise
