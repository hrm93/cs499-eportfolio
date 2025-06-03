# spatial_utils.py

import geopandas as gpd
import logging
import warnings

from pyproj import CRS
from shapely.geometry.base import BaseGeometry

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
