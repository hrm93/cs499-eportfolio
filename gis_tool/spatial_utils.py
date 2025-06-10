# spatial_utils.py

import geopandas as gpd
import logging
import math
import warnings

from pyproj import CRS
from shapely.geometry.base import BaseGeometry
from typing import Optional, Dict, Any

from . import config

logger = logging.getLogger("gis_tool")


def validate_and_reproject_crs(
    gdf: gpd.GeoDataFrame,
    reference_crs,
    dataset_name: str
) -> gpd.GeoDataFrame:
    """
    Ensures the GeoDataFrame shares the same CRS as the reference.
    If not, reprojects to the reference CRS.

    :param gdf: GeoDataFrame to validate and reproject.
    :param reference_crs: The target CRS.
    :param dataset_name: Name of the dataset for logging.
    :return: GeoDataFrame with CRS matching reference_crs.
    """
    if gdf.crs is None:
        logger.error(f"[CRS Validation] Dataset '{dataset_name}' has no CRS defined. "
                      f"Please ensure it has a valid CRS.")
        raise ValueError(f"Dataset '{dataset_name}' is missing a CRS.")

    # Normalize CRS objects for comparison
    target_crs = CRS(reference_crs)
    current_crs = gdf.crs

    if not current_crs.equals(target_crs):
        logger.warning(
            f"[CRS Validation] Dataset '{dataset_name}' CRS mismatch: "
            f"{current_crs} → {target_crs}. Auto-reprojecting."
        )
        gdf = gdf.to_crs(target_crs)
    else:
        logger.info(
            f"[CRS Validation] Dataset '{dataset_name}' already in target CRS ({target_crs})."
        )

    return gdf


def validate_geometry_column(
    gdf: gpd.GeoDataFrame,
    dataset_name: str,
    allowed_geom_types=None
) -> gpd.GeoDataFrame:
    """
    Validates that the GeoDataFrame has a valid 'geometry' column.
    Checks for empty geometries and filters by allowed geometry types if specified.

    :param gdf: GeoDataFrame to validate.
    :param dataset_name: Name of the dataset for logging.
    :param allowed_geom_types: List or set of allowed geometry types (e.g. ['Point', 'LineString']).
    :return: The same GeoDataFrame if valid, else raises ValueError.
    """
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
    return gdf


def validate_geometry_crs(geometry: BaseGeometry, expected_crs: str) -> bool:
    """
    Validate if a shapely geometry matches the expected CRS.
    Since shapely geometries have no CRS, we wrap them in a GeoSeries to check CRS.

    Args:
        geometry (BaseGeometry): Shapely geometry object.
        expected_crs (str): Expected CRS string, e.g., 'EPSG:4326'.

    Returns:
        bool: True if CRS matches expected CRS or geometry is empty; False otherwise.
    """
    try:
        if geometry.is_empty:
            return True

        # Wrap in GeoSeries, assign expected CRS
        gs = gpd.GeoSeries([geometry], crs=expected_crs)
        return gs.crs.to_string() == expected_crs
    except (AttributeError, ValueError, TypeError) as e:
        # Log the specific error for debugging
        logging.getLogger("gis_tool").error(f"CRS validation error: {e}")
        return False


def reproject_geometry_to_crs(geom: BaseGeometry, source_crs: str, target_crs: str) -> BaseGeometry:
    """
    Reproject a single shapely geometry from source_crs to target_crs.

    Args:
        geom (BaseGeometry): The geometry to reproject.
        source_crs (str): The current CRS of the geometry, e.g., 'EPSG:4326'.
        target_crs (str): The target CRS, e.g., 'EPSG:3857'.

    Returns:
        BaseGeometry: The reprojected geometry.
    """
    # Wrap geometry into a GeoSeries to use geopandas reprojection
    geo_series = gpd.GeoSeries([geom], crs=source_crs)
    reprojected_series = geo_series.to_crs(target_crs)
    return reprojected_series.iloc[0]


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


def assert_geodataframes_equal(gdf1, gdf2, tol=1e-6):
    """
      Assert that two GeoDataFrames are equal in terms of CRS, length, and geometry,
      with geometries compared using an exact match within a given tolerance.

      Parameters
      ----------
      gdf1 : geopandas.GeoDataFrame
          The first GeoDataFrame to compare.
      gdf2 : geopandas.GeoDataFrame
          The second GeoDataFrame to compare.
      tol : float, optional
          Tolerance for geometry equality comparison (default is 1e-6).

      Raises
      ------
      AssertionError
          If any of the checks (type, CRS, length, geometry equality) fail.
      """
    logger.info("Comparing two GeoDataFrames for equality.")
    assert isinstance(gdf1, gpd.GeoDataFrame), "First input is not a GeoDataFrame"
    assert isinstance(gdf2, gpd.GeoDataFrame), "Second input is not a GeoDataFrame"

    # Normalize CRS for comparison
    crs1 = gdf1.crs
    crs2 = gdf2.crs
    assert crs1 is not None and crs2 is not None, "One or both GeoDataFrames lack CRS"
    assert CRS(crs1).equals(CRS(crs2)), f"CRS mismatch: {crs1} != {crs2}"

    assert len(gdf1) == len(gdf2), "GeoDataFrames have different lengths"

    for i, (geom1, geom2) in enumerate(zip(gdf1.geometry, gdf2.geometry)):
        # Check for None or invalid geometry
        if not isinstance(geom1, BaseGeometry) or geom1.is_empty or geom1 is None:
            raise AssertionError(f"Invalid geometry at index {i} in first GeoDataFrame")
        if not isinstance(geom2, BaseGeometry) or geom2.is_empty or geom2 is None:
            raise AssertionError(f"Invalid geometry at index {i} in second GeoDataFrame")

        equal = geom1.equals_exact(geom2, tolerance=tol)
        logger.debug(f"Geometry check at index {i}: {'equal' if equal else 'not equal'}")
        assert equal, f"Geometries at index {i} differ"

    logger.info("GeoDataFrames are equal.")


def buffer_intersects_gas_lines(buffer_geom: BaseGeometry, gas_lines_gdf: gpd.GeoDataFrame) -> bool:
    """
    Check if the buffer geometry intersects with any gas line geometries.

    Parameters:
        buffer_geom (BaseGeometry): The buffer geometry to test.
        gas_lines_gdf (GeoDataFrame): GeoDataFrame containing gas line geometries.

    Returns:
        bool: True if there is at least one intersection, False otherwise.
    """
    if buffer_geom is None or buffer_geom.is_empty:
        logger.warning("Empty or None buffer geometry received for intersection check.")
        return False

    # Assumes gas_lines_gdf and buffer_geom share the same CRS

    try:
        # Use spatial index to quickly find candidate gas lines intersecting bounding box
        possible_matches_index = list(gas_lines_gdf.sindex.intersection(buffer_geom.bounds))
        possible_matches = gas_lines_gdf.iloc[possible_matches_index]

        # Vectorized precise intersection check on candidates
        intersects = possible_matches.geometry.intersects(buffer_geom).any()

        logger.debug(f"Buffer intersects gas lines: {intersects}")
        return intersects
    except Exception as e:
        logger.error(f"Error checking intersection between buffer and gas lines: {e}")

        # Fallback: check all geometries (brute force)
        for gas_line_geom in gas_lines_gdf.geometry:
            if gas_line_geom.intersects(buffer_geom):
                logger.debug("Buffer intersects gas line found in fallback loop.")
                return True

        return False


def is_finite_geometry(geom: Optional[Dict[str, Any]]) -> bool:
    """
    Check if a geometry dictionary (GeoJSON-style) has only finite coordinate values.

    Args:
        geom (Optional[Dict[str, Any]]): Geometry dictionary following GeoJSON structure.

    Returns:
        bool: True if all coordinates are finite numbers, False otherwise.
    """
    if not geom or "coordinates" not in geom:
        logger.debug("Geometry is missing or does not contain coordinates.")
        return False

    coords = geom["coordinates"]


    def check_finite_coords(coords_part):
        """
        Recursively check if all numeric values in a coordinate structure are finite.
        """
        if isinstance(coords_part, (list, tuple)):
            return all(check_finite_coords(item) for item in coords_part)
        elif isinstance(coords_part, (int, float)):
            return math.isfinite(coords_part)
        else:
            return False

    finite_check = check_finite_coords(coords)
    if not finite_check:
        logger.warning(f"Geometry has non-finite coordinates: {geom}")

    return finite_check
