# buffer_processor.py

import logging
from typing import Optional, Callable, Any, List, Sequence, Dict
from concurrent.futures import ProcessPoolExecutor, as_completed

import geopandas as gpd
import pandas as pd
import fiona.errors
from shapely.geometry import Point, mapping
from shapely.geometry.base import BaseGeometry
from shapely.errors import TopologicalError

from gis_tool import config

logger = logging.getLogger("gis_tool")


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


def ensure_projected_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Ensure the GeoDataFrame has a projected CRS. If not, reproject to DEFAULT_CRS.

    Args:
        gdf (GeoDataFrame): Input GeoDataFrame to check/reproject.

    Returns:
        GeoDataFrame: Projected GeoDataFrame in DEFAULT_CRS.

    Raises:
        ValueError: If input GeoDataFrame has no CRS defined.
    """
    logger.debug(f"ensure_projected_crs called with CRS: {gdf.crs}")
    if gdf.crs is None:
        raise ValueError("Input GeoDataFrame has no CRS defined.")
    if not gdf.crs.is_projected:
        logger.info(f"Reprojecting from {gdf.crs} to {config.DEFAULT_CRS}")
        gdf = gdf.to_crs(config.DEFAULT_CRS)
    else:
        logger.debug("GeoDataFrame already has projected CRS.")
    return gdf


def buffer_geometry(geom, buffer_distance_m):
    """
    Creates a buffer around a given geometry by the specified distance.

    Parameters:
    - geom: a geometry object (e.g., from Shapely) to buffer
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

def buffer_geometry_helper(geom_and_distance):
    """
    Helper function to unpack arguments and call buffer_geometry.

    Parameters:
    - geom_and_distance: tuple containing
        - geom: a geometry object to buffer
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
    """
    logger.debug("subtract_park_from_geom called.")
    try:
        for park_geom in parks_geoms:
            if buffer_geom.is_empty:
                logger.warning("Buffer geometry is empty during park subtraction; skipping.")
                break
            buffer_geom = buffer_geom.difference(park_geom)
        logger.debug("Park geometries subtracted from buffer.")
        return buffer_geom
    except Exception as e:
        logger.error(f"Error subtracting park geometry: {e}")
        return buffer_geom  # Return original if error


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


def parallel_process(
    func: Callable,
    items: Sequence,
    max_workers: int = None,
) -> List[Any]:
    """
    Run a function on a list of items in parallel using ProcessPoolExecutor.

    Args:
        func: Function to run on each item.
        items: Iterable of input items.
        max_workers: Max number of worker processes; defaults to number of processors.

    Returns:
        List of results in the order they complete (not guaranteed original order).
    """
    logger.info(f"parallel_process called with {len(items)} items.")
    results = [None] * len(items)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(func, item): idx for idx, item in enumerate(items)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
                logger.debug(f"parallel_process item {idx} completed.")
            except Exception as e:
                logger.error(f"Error in parallel_process for item {idx}: {e}")
                results[idx] = None
    return results


def subtract_parks_from_buffer(
    buffer_gdf: gpd.GeoDataFrame,
    parks_path: str,
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
        parks_gdf = gpd.read_file(parks_path)
        logger.debug("Parks layer loaded successfully.")
        if parks_gdf.crs is None:
            logger.warning("Parks layer has no CRS. Assigning buffer CRS.")
            parks_gdf.set_crs(buffer_gdf.crs, inplace=True)

        if parks_gdf.crs != buffer_gdf.crs:
            logger.info("Reprojecting parks layer to match buffer CRS.")
            parks_gdf = parks_gdf.to_crs(buffer_gdf.crs)

        parks_gdf['geometry'] = parks_gdf.geometry.apply(fix_geometry)
        buffer_gdf['geometry'] = buffer_gdf.geometry.apply(fix_geometry)

        parks_gdf = parks_gdf[parks_gdf.geometry.notnull() & parks_gdf.geometry.is_valid]
        buffer_gdf = buffer_gdf[buffer_gdf.geometry.notnull() & buffer_gdf.geometry.is_valid]

        parks_geoms = list(parks_gdf.geometry)
        logger.debug(f"Number of valid park geometries: {len(parks_geoms)}")

        if use_multiprocessing:
            logger.info("Subtracting parks using multiprocessing.")
            args = [(geom, parks_geoms) for geom in buffer_gdf.geometry]
            buffer_gdf['geometry'] = parallel_process(subtract_park_from_geom_helper, args)
        else:
            logger.info("Subtracting parks sequentially.")
            buffer_gdf['geometry'] = buffer_gdf.geometry.apply(lambda geom: subtract_park_from_geom(geom, parks_geoms))

        buffer_gdf['geometry'] = buffer_gdf.geometry.apply(fix_geometry)
        buffer_gdf = buffer_gdf[
            buffer_gdf.geometry.notnull() & buffer_gdf.geometry.is_valid & ~buffer_gdf.geometry.is_empty]

        logger.info(f"Subtraction complete. Remaining features: {len(buffer_gdf)}")
        return buffer_gdf
    except Exception as e:
        logger.exception(f"Error in subtract_parks_from_buffer: {e}")
        raise


def simplify_geometry(geom: Point, tolerance: float = 0.00001) -> Dict:
    """
    Simplify a Point geometry to reduce floating point precision issues.

    Uses Shapely's simplify method with topology preservation to reduce
    the complexity of the geometry while maintaining its shape.

    Args:
        geom (Point): The input Shapely Point geometry to simplify.
        tolerance (float, optional): The tolerance threshold for simplification.
            Defaults to 0.00001.

    Returns:
        dict: A GeoJSON-like mapping dictionary of the simplified geometry.
    """
    # Simplify geometry to avoid floating point precision issues
    logger.debug(f"simplify_geometry called with tolerance: {tolerance}")
    simplified = geom.simplify(tolerance, preserve_topology=True)
    logger.debug("Geometry simplified.")
    return mapping(simplified)

def create_buffer_with_geopandas(
    input_gas_lines_path: str,
    buffer_distance_ft: Optional[float] = None,
    parks_path: Optional[str] = None,
    use_multiprocessing: bool = False,
) -> gpd.GeoDataFrame:
    """
    Create a buffer polygon around gas lines features using GeoPandas.

    Args:
        input_gas_lines_path (str): File path to input gas lines layer (shapefile, GeoPackage, etc.).
        buffer_distance_ft (float, optional): Buffer distance in feet.
        parks_path (str, optional): File path to park polygons to subtract from buffer.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing the buffered geometries (with parks subtracted if applicable).
        :param input_gas_lines_path:
        :param parks_path:
        :param buffer_distance_ft:
        :param use_multiprocessing:
    """
    logger.info(f"create_buffer_with_geopandas called with input: {input_gas_lines_path}")
    if buffer_distance_ft is None:
        buffer_distance_ft = config.DEFAULT_BUFFER_DISTANCE_FT
    buffer_distance_m = buffer_distance_ft * 0.3048
    logger.debug(f"Buffer distance in meters: {buffer_distance_m}")

    try:
        gas_lines_gdf = gpd.read_file(input_gas_lines_path)
        logger.debug("Gas lines layer loaded.")
        if gas_lines_gdf.crs is None:
            logger.warning("Input gas lines layer has no CRS; assigning default.")
            gas_lines_gdf = gas_lines_gdf.set_crs(config.DEFAULT_CRS)
        gas_lines_gdf = ensure_projected_crs(gas_lines_gdf)

        if use_multiprocessing:
            logger.info("Buffering geometries with multiprocessing.")
            args = [(geom, buffer_distance_m) for geom in gas_lines_gdf.geometry]
            gas_lines_gdf['geometry'] = parallel_process(buffer_geometry_helper, args)
        else:
            logger.info("Buffering geometries sequentially.")
            gas_lines_gdf['geometry'] = gas_lines_gdf.geometry.buffer(buffer_distance_m)

        if parks_path:
            logger.info(f"Subtracting parks from buffers using parks layer at {parks_path}")
            gas_lines_gdf = subtract_parks_from_buffer(gas_lines_gdf, parks_path)

        logger.debug("Buffering complete.")
        return gas_lines_gdf
    except Exception as e:
        logger.exception(f"Error in create_buffer_with_geopandas: {e}")
        raise


def merge_buffers_into_planning_file(
    unique_output_buffer: str,
    future_development_feature_class: str,
    point_buffer_distance: float = 10.0,
) -> gpd.GeoDataFrame:
    """
    Merge buffer polygons into a Future Development planning layer by appending features.

    ⚠️ WARNING: This function overwrites the input Future Development shapefile.

    Args:
        unique_output_buffer (str): File path to the buffer polygons shapefile.
        future_development_feature_class (str): File path to the Future Development shapefile.
        point_buffer_distance (float): Buffer distance in meters to convert non-polygon features to polygons.

    Returns:
        gpd.GeoDataFrame: The merged GeoDataFrame.
    """
    logger.info(f"merge_buffers_into_planning_file called with buffer: {unique_output_buffer}")
    try:
        buffer_gdf = gpd.read_file(unique_output_buffer)
        future_dev_gdf = gpd.read_file(future_development_feature_class)

        if buffer_gdf.empty:
            logger.warning("Buffer GeoDataFrame is empty; no geometries to merge.")
            logger.info(
                f"No update performed on '{future_development_feature_class}'; existing data remains unchanged.")
            # Return the original future development GeoDataFrame unchanged
            return future_dev_gdf

        if future_dev_gdf.empty:
            logger.warning("Future Development GeoDataFrame is empty; result will contain only buffer polygons.")

        if not future_dev_gdf.crs or future_dev_gdf.crs.to_string() == '':
            logger.warning("Future Development layer missing CRS; defaulting to EPSG:4326.")
            future_dev_gdf = future_dev_gdf.set_crs(config.GEOGRAPHIC_CRS, allow_override=True)
        if not buffer_gdf.crs or buffer_gdf.to_string() == '':
            logger.warning("Buffer layer missing CRS; defaulting to EPSG:32610.")
            buffer_gdf = buffer_gdf.set_crs(config.BUFFER_LAYER_CRS, allow_override=True)

        if buffer_gdf.crs != future_dev_gdf.crs:
            logger.info(f"Reprojecting buffer from {buffer_gdf.crs} to {future_dev_gdf.crs}")
            buffer_gdf = buffer_gdf.to_crs(future_dev_gdf.crs)

        if not buffer_gdf.geom_type.isin(['Polygon', 'MultiPolygon']).all():
            raise ValueError("Buffer shapefile must contain only polygon or multipolygon geometries.")

        unique_future_geom_types = future_dev_gdf.geom_type.unique()
        if len(unique_future_geom_types) != 1:
            raise ValueError(f"Future Development shapefile has mixed geometry types: {unique_future_geom_types}")

        future_geom_type = unique_future_geom_types[0]

        if future_geom_type not in ['Polygon', 'MultiPolygon']:
            logger.info(f"Converting Future Development geometries from {future_geom_type} to polygons by buffering with {point_buffer_distance} meters.")
            if future_geom_type in ['Point', 'LineString']:
                original_crs = future_dev_gdf.crs
                projected = future_dev_gdf.to_crs(epsg=3857)
                buffered = projected.geometry.buffer(point_buffer_distance).buffer(0)
                future_dev_gdf['geometry'] = gpd.GeoSeries(buffered, crs=projected.crs).to_crs(original_crs)
            else:
                raise ValueError(f"Unsupported Future Development geometry type '{future_geom_type}' for conversion.")

        # Clean and fix geometries
        future_dev_gdf['geometry'] = future_dev_gdf.geometry.apply(fix_geometry)
        buffer_gdf['geometry'] = buffer_gdf.geometry.apply(fix_geometry)

        future_dev_gdf = future_dev_gdf[future_dev_gdf.geometry.notnull() & future_dev_gdf.geometry.is_valid & ~future_dev_gdf.geometry.is_empty]
        buffer_gdf = buffer_gdf[buffer_gdf.geometry.notnull() & buffer_gdf.geometry.is_valid & ~buffer_gdf.geometry.is_empty]

        driver = config.get_driver_from_extension(future_development_feature_class)

        frames = [df for df in [future_dev_gdf, buffer_gdf] if not df.empty]
        merged_gdf = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=future_dev_gdf.crs)

        logger.info(f"Merged GeoDataFrame has {len(merged_gdf)} features after merging and cleaning.")

        if merged_gdf.empty:
            logger.warning(f"Merged GeoDataFrame is empty; skipping writing to {future_development_feature_class}")
            # Return empty GeoDataFrame with correct CRS without writing file
            return gpd.GeoDataFrame(geometry=[], crs=future_dev_gdf.crs)
        else:
            merged_gdf.to_file(future_development_feature_class, driver=driver)
            logger.info(f"Merged data saved to {future_development_feature_class}")
            return merged_gdf

    except (OSError, IOError, ValueError, fiona.errors.FionaError) as e:
        logger.exception(f"Error in merge_buffers_into_planning_file: {e}")
        raise
