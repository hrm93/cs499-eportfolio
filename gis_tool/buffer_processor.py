# buffer_processor.py

import logging
import warnings
from typing import Optional, Callable, Any, List, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed

import geopandas as gpd
import pandas as pd
import fiona.errors
from shapely.geometry import Polygon
from shapely.geometry.base import BaseGeometry

from gis_tool import config
from gis_tool.utils import convert_ft_to_m, clean_geodataframe, fix_geometry

logger = logging.getLogger("gis_tool")


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
        warnings.warn("Input GeoDataFrame has no CRS defined. Cannot proceed without CRS.", UserWarning)
        raise ValueError("Input GeoDataFrame has no CRS defined.")
    if not gdf.crs.is_projected:
        warnings.warn(f"Input CRS {gdf.crs} is not projected. Reprojecting to {config.DEFAULT_CRS}.", UserWarning)
        logger.info(f"Reprojecting from {gdf.crs} to {config.DEFAULT_CRS}")
        gdf = gdf.to_crs(config.DEFAULT_CRS)
    else:
        logger.debug("GeoDataFrame already has projected CRS.")
    return gdf


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
        List of results in the original order of input items.
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
            warnings.warn("Parks layer has no CRS. Assigning buffer CRS.", UserWarning)
            logger.warning("Parks layer has no CRS. Assigning buffer CRS.")
            parks_gdf.set_crs(buffer_gdf.crs, inplace=True)

        if parks_gdf.crs != buffer_gdf.crs:
            warnings.warn(f"Parks CRS {parks_gdf.crs} differs from buffer CRS {buffer_gdf.crs}. Reprojecting.", UserWarning)
            logger.info("Reprojecting parks layer to match buffer CRS.")
            parks_gdf = parks_gdf.to_crs(buffer_gdf.crs)

        # Fix geometries to ensure validity
        parks_gdf['geometry'] = parks_gdf.geometry.apply(fix_geometry)
        buffer_gdf['geometry'] = buffer_gdf.geometry.apply(fix_geometry)

        # Initial geometry validation to clean up inputs
        parks_gdf = parks_gdf[parks_gdf.geometry.notnull() & parks_gdf.geometry.is_valid]
        buffer_gdf = buffer_gdf[buffer_gdf.geometry.notnull() & buffer_gdf.geometry.is_valid]

        if parks_gdf.empty:
            warnings.warn("No valid park geometries found for subtraction.", UserWarning)
            logger.warning("No valid park geometries found for subtraction.")

        parks_geoms = list(parks_gdf.geometry)
        logger.debug(f"Number of valid park geometries: {len(parks_geoms)}")

        if use_multiprocessing:
            logger.info("Subtracting parks using multiprocessing.")
            args = [(geom, parks_geoms) for geom in buffer_gdf.geometry]
            buffer_gdf['geometry'] = parallel_process(subtract_park_from_geom_helper, args)
        else:
            logger.info("Subtracting parks sequentially.")
            buffer_gdf['geometry'] = buffer_gdf.geometry.apply(lambda geom: subtract_park_from_geom(geom, parks_geoms))

        # Fix geometries after subtraction
        buffer_gdf['geometry'] = buffer_gdf.geometry.apply(fix_geometry)

        # FINAL consistent geometry validation before returning
        buffer_gdf = buffer_gdf[buffer_gdf.geometry.is_valid & ~buffer_gdf.geometry.is_empty]

        logger.info(f"Subtraction complete. Remaining features: {len(buffer_gdf)}")
        return buffer_gdf
    except Exception as e:
        logger.exception(f"Error in subtract_parks_from_buffer: {e}")
        warnings.warn(f"Error subtracting parks: {e}", UserWarning)
        raise


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
        use_multiprocessing (bool, optional): Whether to use parallel processing to speed up buffering and subtraction.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing the buffered geometries (with parks subtracted if applicable).

    Notes:
        - Parallel processing is recommended for large datasets or complex geometries to improve performance.
        - Using multiprocessing with GeoPandas/Shapely may introduce serialization overhead or issues with very large or complex geometries.
        - For small datasets or simple features, serial processing may be faster and more stable.
        - Alternative parallel backends like Dask or GeoPandas' experimental parallel features may offer improved scalability in the future.
    """
    logger.info(f"create_buffer_with_geopandas called with input: {input_gas_lines_path}")
    if buffer_distance_ft is None:
        buffer_distance_ft = config.DEFAULT_BUFFER_DISTANCE_FT
    buffer_distance_m = convert_ft_to_m(buffer_distance_ft)
    logger.debug(f"Buffer distance in meters: {buffer_distance_m}")

    try:
        gas_lines_gdf = gpd.read_file(input_gas_lines_path)
        logger.debug("Gas lines layer loaded.")

        if gas_lines_gdf.crs is None:
            warnings.warn("Input gas lines layer has no CRS. Assigning default CRS.", UserWarning)
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
            warnings.warn("Subtracting parks from buffers. Ensure parks data is clean and valid.", UserWarning)
            logger.info(f"Subtracting parks from buffers using parks layer at {parks_path}")
            gas_lines_gdf = subtract_parks_from_buffer(gas_lines_gdf, parks_path)

        logger.debug("Buffering complete.")

        # Clean the GeoDataFrame before final validation
        gas_lines_gdf = clean_geodataframe(gas_lines_gdf)

        # FINAL geometry validation before returning
        gas_lines_gdf = gas_lines_gdf[gas_lines_gdf.geometry.is_valid & ~gas_lines_gdf.geometry.is_empty]

        logger.info(f"Final GeoDataFrame contains {len(gas_lines_gdf)} valid, non-empty geometries.")
        return gas_lines_gdf

    except Exception as e:
        logger.exception(f"Error in create_buffer_with_geopandas: {e}")
        warnings.warn(f"Error creating buffer: {e}", UserWarning)
        raise


def merge_buffers_into_planning_file(
    unique_output_buffer: str,
    future_development_feature_class: str,
    point_buffer_distance: float = 10.0,
) -> gpd.GeoDataFrame:
    """
       Merge buffer polygons into a Future Development planning layer by appending features.

       ⚠️ USER-FACING WARNINGS:
       - This function overwrites the input Future Development shapefile.
       - No buffer geometries found; no update applied to Future Development layer.
       - Future Development layer is empty; merged output will contain only buffer polygons.
       - Future Development layer missing CRS; assigning default geographic CRS EPSG:4326.
       - Buffer layer missing CRS; assigning default projected CRS EPSG:32610.
       - Buffer layer CRS differs from Future Development CRS; reprojecting buffer layer.

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
            warnings.warn("No buffer geometries found; no update applied to Future Development layer.", UserWarning)
            logger.warning("Buffer GeoDataFrame is empty; no geometries to merge.")
            logger.info(
                f"No update performed on '{future_development_feature_class}'; existing data remains unchanged.")
            # Return the original future development GeoDataFrame unchanged
            return future_dev_gdf

        if future_dev_gdf.empty:
            warnings.warn("Future Development layer is empty; merged output will contain only buffer polygons.",
                          UserWarning)
            logger.warning("Future Development GeoDataFrame is empty; result will contain only buffer polygons.")

        if not future_dev_gdf.crs or future_dev_gdf.crs.to_string() == '':
            warnings.warn("Future Development layer missing CRS; assigning default geographic CRS EPSG:4326.",
                          UserWarning)
            logger.warning("Future Development layer missing CRS; defaulting to EPSG:4326.")
            future_dev_gdf = future_dev_gdf.set_crs(config.GEOGRAPHIC_CRS, allow_override=True)

        if not buffer_gdf.crs or buffer_gdf.to_string() == '':
            warnings.warn("Buffer layer missing CRS; assigning default projected CRS EPSG:32610.", UserWarning)
            logger.warning("Buffer layer missing CRS; defaulting to EPSG:32610.")
            buffer_gdf = buffer_gdf.set_crs(config.BUFFER_LAYER_CRS, allow_override=True)

        if buffer_gdf.crs != future_dev_gdf.crs:
            warnings.warn("Buffer layer CRS differs from Future Development CRS; reprojecting buffer layer.",
                          UserWarning)
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
