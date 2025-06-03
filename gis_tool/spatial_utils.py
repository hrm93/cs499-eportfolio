# spatial_utils.py

import geopandas as gpd
import logging
import warnings

from . import config

logger = logging.getLogger("gis_tool")


def validate_and_reproject_crs(
    gdf: gpd.GeoDataFrame,
    reference_crs: str,
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

    if gdf.crs != reference_crs:
        logger.warning(
            f"[CRS Validation] Dataset '{dataset_name}' CRS mismatch: "
            f"{gdf.crs} → {reference_crs}. Auto-reprojecting."
        )
        gdf = gdf.to_crs(reference_crs)
    else:
        logger.info(
            f"[CRS Validation] Dataset '{dataset_name}' already in target CRS ({reference_crs})."
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
