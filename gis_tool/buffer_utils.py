### buffer_utils
import logging
import warnings
import geopandas as gpd

from shapely.geometry import Polygon
from shapely.geometry.base import BaseGeometry

from gis_tool.geometry_cleaning import fix_geometry

logger = logging.getLogger("gis_tool")


def buffer_geometry(geom: BaseGeometry, buffer_distance_m: float) -> BaseGeometry | None:
    """
    Creates a buffer around a given geometry by the specified distance.

    Parameters:
    - geom: a geometry object (BaseGeometry) to buffer
    - buffer_distance_m: buffer distance in meters (float or int)

    Returns:
    - A new geometry buffered by buffer_distance_m, or None if an error occurs.
    """
    logger.debug(f"buffer_geometry called with geometry: {geom} and buffer_distance_m: {buffer_distance_m}")
    try:
        buffered = geom.buffer(buffer_distance_m)
        logger.debug("Buffering successful.")
        return buffered
    except Exception as e:
        logger.error(f"Buffering error: {e}")
        return None


def buffer_geometry_helper(geom_and_distance: tuple[BaseGeometry, float]) -> BaseGeometry | None:
    """
    Helper function to unpack arguments and call buffer_geometry.

    Parameters:
    - geom_and_distance: tuple containing
        - geom: a geometry object (BaseGeometry) to buffer
        - distance: buffer distance in meters

    Returns:
    - Buffered geometry result from buffer_geometry(geom, distance)
    """
    geom, distance = geom_and_distance
    logger.debug(f"buffer_geometry_helper called with distance: {distance}")
    return buffer_geometry(geom, distance)


def subtract_park_from_geom(buffer_geom, parks_geoms):
    """
     Subtract all park geometries from a single buffer geometry.

    Parameters:
    - buffer_geom: geometry to subtract from
    - parks_geoms: iterable of park geometries to subtract

    Returns:
    - Geometry after subtraction, or empty Polygon if input invalid or error occurs.
    """
    logger.debug("subtract_park_from_geom called.")
    # Fix initial geometry
    buffer_geom = fix_geometry(buffer_geom)
    if buffer_geom is None or buffer_geom.is_empty:
        logger.warning("Input buffer geometry is invalid or empty; returning empty Polygon.")
        warnings.warn("Input buffer geometry is invalid or empty. Buffer will be empty.", UserWarning)
        return Polygon()

    try:
        for park_geom in parks_geoms:
            park_geom = fix_geometry(park_geom)
            if park_geom is None or park_geom.is_empty:
                logger.debug("Skipping invalid or empty park geometry during subtraction.")
                continue
            if buffer_geom.is_empty:
                logger.warning("Buffer geometry became empty during park subtraction; stopping.")
                warnings.warn("Buffer geometry became empty during park subtraction.", UserWarning)
                break
            buffer_geom = buffer_geom.difference(park_geom)
            buffer_geom = fix_geometry(buffer_geom)
            if buffer_geom is None or buffer_geom.is_empty:
                logger.warning("Buffer geometry invalid or empty after subtraction; returning empty Polygon.")
                warnings.warn("Buffer geometry invalid or empty after park subtraction.", UserWarning)
                return Polygon()
        logger.debug("Park geometries subtracted from buffer.")
        return buffer_geom
    except Exception as e:
        logger.error(f"Error subtracting park geometry: {e}")
        warnings.warn(f"Error subtracting park geometry: {e}", UserWarning)
        return Polygon()


def subtract_park_from_geom_helper(geom_and_parks):
    """
    Helper function to unpack arguments and call subtract_park_from_geom.

    Parameters:
    - geom_and_parks: tuple containing
        - geom: a geometry object to subtract from
        - parks_geoms: a collection of park geometries to subtract

    Returns:
    - Result of subtract_park_from_geom(geom, parks_geoms)
    """
    geom, parks_geoms = geom_and_parks
    logger.debug("subtract_park_from_geom_helper called.")
    return subtract_park_from_geom(geom, parks_geoms)


def log_and_filter_invalid_geometries(gdf: gpd.GeoDataFrame, layer_name: str) -> gpd.GeoDataFrame:
    """Helper to log and filter out null, empty, and invalid geometries from a GeoDataFrame."""
    null_or_empty = gdf.geometry.is_empty | gdf.geometry.isnull()
    if null_or_empty.any():
        logger.warning(f"{layer_name}: {null_or_empty.sum()} features with empty or null geometries excluded.")
    gdf = gdf[~null_or_empty]

    invalid = ~gdf.geometry.is_valid
    if invalid.any():
        logger.warning(f"{layer_name}: {invalid.sum()} invalid geometries excluded.")
    gdf = gdf[gdf.geometry.is_valid]

    return gdf
