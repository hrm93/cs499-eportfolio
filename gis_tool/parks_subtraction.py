"""
parks_subtraction.py

Handles subtraction of park polygons from buffered gas line polygons.
Supports optional multiprocessing and includes geometry validation/fixing.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import warnings

from rich.progress import Progress
from typing import Optional, Union

from pyproj import CRS
import geopandas as gpd

from .spatial_utils import validate_and_reproject_crs
from gis_tool.geometry_cleaning import fix_geometry
from gis_tool.parallel_utils import parallel_process
from gis_tool.buffer_utils import (
    subtract_park_from_geom,
    log_and_filter_invalid_geometries,
)

# Set up logger for this module
logger = logging.getLogger("gis_tool.parks_subtraction")


def subtract_park_from_geom_spatial_worker(args):
    """
    Worker-safe function to subtract only relevant park geometries from a buffer geometry
    using a spatial index built locally within the worker process.
    """
    buffer_geom, parks_geoms = args

    if buffer_geom is None or buffer_geom.is_empty:
        return buffer_geom

    parks_gdf_local = gpd.GeoDataFrame(geometry=parks_geoms)
    sindex = parks_gdf_local.sindex

    candidate_idx = list(sindex.intersection(buffer_geom.bounds))
    candidate_parks = parks_gdf_local.iloc[candidate_idx].geometry.tolist()

    return subtract_park_from_geom(buffer_geom, candidate_parks)


def subtract_parks_from_buffer(
    buffer_gdf: gpd.GeoDataFrame,
    parks_path: Optional[str] = None,
    use_multiprocessing: bool = False,
    linestring_buffer_distance: float = 5.0,
) -> gpd.GeoDataFrame:
    """
    Subtract park polygons from buffer polygons using spatial index and optional multiprocessing.

    Args:
        buffer_gdf (GeoDataFrame): GeoDataFrame of buffered gas lines.
        parks_path (Optional[str]): File path to park polygons layer.
        use_multiprocessing (bool): If True, use multiprocessing.
        linestring_buffer_distance (float): Buffer distance for LineStrings.

    Returns:
        GeoDataFrame: Updated GeoDataFrame with parks subtracted.
    """
    logger.info(f"subtract_parks_from_buffer called with parks_path: {parks_path}")

    try:
        if parks_path is None:
            logger.info("No parks path provided, returning buffer unchanged.")
            return buffer_gdf.copy()

        parks_gdf = gpd.read_file(parks_path)
        logger.debug("Parks layer loaded successfully.")

        parks_gdf = validate_and_reproject_crs(parks_gdf, buffer_gdf.crs, "parks", default_crs="EPSG:32633")

        if 'LineString' in parks_gdf.geom_type.unique():
            logger.info(f"Buffering LineStrings by {linestring_buffer_distance} to convert to polygons")
            parks_gdf.loc[parks_gdf.geom_type == 'LineString', 'geometry'] = \
                parks_gdf.loc[parks_gdf.geom_type == 'LineString', 'geometry'].buffer(linestring_buffer_distance)

        allowed_park_types = ['Polygon', 'MultiPolygon']
        invalid_park_types = parks_gdf.geom_type[~parks_gdf.geom_type.isin(allowed_park_types)]
        if not invalid_park_types.empty:
            logger.warning(
                f"Unsupported geometry types in parks layer: {invalid_park_types.unique()}. These will be excluded."
            )
            parks_gdf = parks_gdf[parks_gdf.geom_type.isin(allowed_park_types)]

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

        if use_multiprocessing:
            logger.info("Subtracting parks using multiprocessing with spatial index.")

            args = [(geom, parks_geoms) for geom in buffer_gdf.geometry]
            buffer_gdf['geometry'] = parallel_process(subtract_park_from_geom_spatial_worker, args)

        else:
            logger.info("Subtracting parks sequentially with spatial index and progress bar.")
            updated_geometries = []

            # Build shared spatial index once for sequential use
            parks_sindex = parks_gdf.sindex

            with Progress() as progress:
                task = progress.add_task("[cyan]Subtracting parks from buffers...", total=len(buffer_gdf))

                for geom in buffer_gdf.geometry:
                    if geom is None or geom.is_empty:
                        updated = geom
                    else:
                        candidate_idx = list(parks_sindex.intersection(geom.bounds))
                        candidate_parks = parks_gdf.iloc[candidate_idx].geometry.tolist()
                        updated = subtract_park_from_geom(geom, candidate_parks)

                    updated_geometries.append(updated)
                    progress.update(task, advance=1)

            buffer_gdf['geometry'] = updated_geometries

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
