"""
Buffer processing module for merging buffer polygons into a Future Development planning layer.

This module provides functionality to merge buffer polygons with existing planning features,
ensuring coordinate reference systems (CRS) compatibility and geometry validity.

Key features:
- Validation and reprojection of CRS
- Geometry type checks and conversion of points/lines to polygons via buffering
- Geometry cleaning and fixing
- Merging GeoDataFrames and saving to file
- User-facing warnings about potentially destructive operations

Exceptions related to file IO, geometry types, and CRS issues are logged and propagated.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import warnings
from typing import Optional

import geopandas as gpd
import pandas as pd
import fiona.errors

from .spatial_utils import validate_and_reproject_crs, ensure_projected_crs
from gis_tool import config
from gis_tool.geometry_cleaning import fix_geometry
from gis_tool.parks_subtraction import CRSLike

logger = logging.getLogger("gis_tool.buffer_processor")


def merge_buffers_into_planning_file(
    unique_output_buffer: str,
    future_development_feature_class: str,
    point_buffer_distance: float = 10.0,
    output_crs: Optional[CRSLike] = None,
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
        output_crs (Optional[CRSLike]): CRS to which the merged GeoDataFrame will be projected before saving.

    Returns:
        gpd.GeoDataFrame: The merged GeoDataFrame.
    """
    logger.info(f"merge_buffers_into_planning_file called with buffer: {unique_output_buffer}")
    try:
        # Load buffer and Future Development layers
        buffer_gdf = gpd.read_file(unique_output_buffer)
        future_dev_gdf = gpd.read_file(future_development_feature_class)

        # Early exit if buffer is empty, warn user
        if buffer_gdf.empty:
            warnings.warn(
                "No buffer geometries found; no update applied to Future Development layer.",
                UserWarning,
            )
            logger.warning("Buffer GeoDataFrame is empty; no geometries to merge.")
            logger.info(
                f"No update performed on '{future_development_feature_class}'; existing data remains unchanged."
            )
            return future_dev_gdf  # Return original unmodified

        # Warn if Future Development layer is empty; result will contain only buffers
        if future_dev_gdf.empty:
            warnings.warn(
                "Future Development layer is empty; merged output will contain only buffer polygons.",
                UserWarning,
            )
            logger.warning(
                "Future Development GeoDataFrame is empty; result will contain only buffer polygons."
            )

        # Assign default CRS if missing on Future Development layer
        if not future_dev_gdf.crs or future_dev_gdf.crs.to_string() == '':
            warnings.warn(
                "Future Development layer missing CRS; assigning default geographic CRS EPSG:4326.",
                UserWarning,
            )
            logger.warning("Future Development layer missing CRS; defaulting to EPSG:4326.")
            future_dev_gdf = future_dev_gdf.set_crs(config.GEOGRAPHIC_CRS, allow_override=True)

        # Assign default CRS if missing on buffer layer
        if not buffer_gdf.crs or buffer_gdf.crs.to_string() == '':
            warnings.warn(
                "Buffer layer missing CRS; assigning default projected CRS EPSG:32610.",
                UserWarning,
            )
            logger.warning("Buffer layer missing CRS; defaulting to EPSG:32610.")
            buffer_gdf = buffer_gdf.set_crs(config.BUFFER_LAYER_CRS, allow_override=True)

        # Validate and reproject buffer to match Future Development CRS
        buffer_gdf = validate_and_reproject_crs(buffer_gdf, future_dev_gdf.crs, "Buffer layer")

        # Ensure projected CRS for geometry operations
        buffer_gdf = ensure_projected_crs(buffer_gdf)
        future_dev_gdf = ensure_projected_crs(future_dev_gdf)

        # Warn and reproject buffer if CRS differs after validation
        if buffer_gdf.crs != future_dev_gdf.crs:
            warnings.warn(
                "Buffer layer CRS differs from Future Development CRS; reprojecting buffer layer.",
                UserWarning,
            )
            logger.info(f"Reprojecting buffer from {buffer_gdf.crs} to {future_dev_gdf.crs}")
            buffer_gdf = buffer_gdf.to_crs(future_dev_gdf.crs)

        # Confirm buffer geometries are polygons or multipolygons
        if not buffer_gdf.geom_type.isin(['Polygon', 'MultiPolygon']).all():
            raise ValueError(
                "Buffer shapefile must contain only polygon or multipolygon geometries."
            )

        # Check and convert Future Development geometries if needed
        if not future_dev_gdf.empty:
            unique_future_geom_types = future_dev_gdf.geom_type.unique()
            if len(unique_future_geom_types) != 1:
                raise ValueError(
                    f"Future Development shapefile has mixed geometry types: {unique_future_geom_types}"
                )
            future_geom_type = unique_future_geom_types[0]

            # Convert points or lines to polygons by buffering
            if future_geom_type not in ['Polygon', 'MultiPolygon']:
                logger.info(
                    f"Converting Future Development geometries from {future_geom_type} to polygons by buffering with {point_buffer_distance} meters."
                )
                if future_geom_type in ['Point', 'LineString']:
                    original_crs = future_dev_gdf.crs
                    projected = future_dev_gdf.to_crs(epsg=3857)
                    buffered = projected.geometry.buffer(point_buffer_distance).buffer(0)
                    future_dev_gdf['geometry'] = (
                        gpd.GeoSeries(buffered, crs=projected.crs).to_crs(original_crs)
                    )
                else:
                    raise ValueError(
                        f"Unsupported Future Development geometry type '{future_geom_type}' for conversion."
                    )

        # Clean and fix geometries for both datasets
        future_dev_gdf['geometry'] = future_dev_gdf.geometry.apply(fix_geometry)
        buffer_gdf['geometry'] = buffer_gdf.geometry.apply(fix_geometry)

        # Filter out null, invalid, or empty geometries
        future_dev_gdf = future_dev_gdf[
            future_dev_gdf.geometry.notnull()
            & future_dev_gdf.geometry.is_valid
            & ~future_dev_gdf.geometry.is_empty
        ]
        buffer_gdf = buffer_gdf[
            buffer_gdf.geometry.notnull()
            & buffer_gdf.geometry.is_valid
            & ~buffer_gdf.geometry.is_empty
        ]

        # Merge GeoDataFrames, excluding empty frames
        frames = [df for df in [future_dev_gdf, buffer_gdf] if not df.empty]
        merged_gdf = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=future_dev_gdf.crs)

        logger.info(f"Merged GeoDataFrame has {len(merged_gdf)} features after merging and cleaning.")

        # Determine output CRS for saving
        if output_crs is None:
            output_crs = buffer_gdf.crs if not buffer_gdf.empty else future_dev_gdf.crs

        # Reproject merged GeoDataFrame to desired output CRS
        merged_gdf = merged_gdf.to_crs(output_crs)

        # Warn and skip writing if merged GeoDataFrame is empty
        if merged_gdf.empty:
            logger.warning(
                f"Merged GeoDataFrame is empty; skipping writing to {future_development_feature_class}"
            )
            return gpd.GeoDataFrame(columns=future_dev_gdf.columns, crs=future_dev_gdf.crs)

        # Determine driver from file extension and save merged shapefile
        driver = config.get_driver_from_extension(future_development_feature_class)
        merged_gdf.to_file(future_development_feature_class, driver=driver)
        logger.info(f"Merged data saved to {future_development_feature_class}")

        return merged_gdf

    except (OSError, IOError, ValueError, fiona.errors.FionaError) as e:
        logger.exception(f"Error in merge_buffers_into_planning_file: {e}")
        raise
