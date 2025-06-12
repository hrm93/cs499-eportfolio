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
logger = logging.getLogger("gis_tool")

def subtract_parks_from_buffer(
    buffer_gdf: gpd.GeoDataFrame,
    parks_path: Optional[str] = None,
    use_multiprocessing: bool = False,
) -> gpd.GeoDataFrame:
    """
      Subtract park polygons from buffer polygons.

      Args:
          buffer_gdf: GeoDataFrame of buffered gas lines (polygons).
          parks_path: File path to park polygons layer.
          use_multiprocessing: If True, subtract parks using multiprocessing.

      Returns:
          GeoDataFrame with parks subtracted from buffer polygons.
      """
    logger.info(f"subtract_parks_from_buffer called with parks_path: {parks_path}")
    try:
        if parks_path is None:
            logger.info("No parks path provided, returning buffer unchanged.")
            return buffer_gdf.copy()

        # Load parks layer
        parks_gdf = gpd.read_file(parks_path)
        logger.debug("Parks layer loaded successfully.")

        # Validate and reproject CRS of parks_gdf using centralized helper
        parks_gdf = validate_and_reproject_crs(parks_gdf, buffer_gdf.crs, "parks")

        # === VALIDATION CHECKS FOR PARKS ===
        allowed_park_types = ['Polygon', 'MultiPolygon']
        invalid_park_types = parks_gdf.geom_type[~parks_gdf.geom_type.isin(allowed_park_types)]
        if not invalid_park_types.empty:
            logger.warning(
                f"Unsupported geometry types in parks layer: {invalid_park_types.unique()}. These will be excluded."
            )
            parks_gdf = parks_gdf[parks_gdf.geom_type.isin(allowed_park_types)]

        # Fix geometries to ensure validity
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

        # Subtract parks from buffers
        if use_multiprocessing:
            logger.info("Subtracting parks using multiprocessing.")
            args = [(geom, parks_geoms) for geom in buffer_gdf.geometry]
            buffer_gdf['geometry'] = parallel_process(subtract_park_from_geom_helper, args)
        else:
            logger.info("Subtracting parks sequentially.")
            buffer_gdf['geometry'] = buffer_gdf.geometry.apply(
                lambda geom: subtract_park_from_geom(geom, parks_geoms)
            )

        # Final geometry fixes and cleanup
        buffer_gdf['geometry'] = buffer_gdf.geometry.apply(fix_geometry)
        buffer_gdf = buffer_gdf[buffer_gdf.geometry.is_valid & ~buffer_gdf.geometry.is_empty]

        logger.info(f"Subtraction complete. Remaining features: {len(buffer_gdf)}")
        return buffer_gdf

    except Exception as e:
        logger.exception(f"Error in subtract_parks_from_buffer: {e}")
        warnings.warn(f"Error subtracting parks: {e}", UserWarning)
        raise

CRSLike = Union[str, CRS]
