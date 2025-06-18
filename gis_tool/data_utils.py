"""
data_utils.py

Utility functions for creating and managing geospatial pipeline feature data.

This module supports the creation of pipeline features as GeoDataFrames and
optionally inserts them into MongoDB if configured. It ensures schema alignment,
CRS consistency, and provides logging for all operations.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
from typing import Union, Optional

import geopandas as gpd
import pandas as pd
from pymongo.collection import Collection
from shapely.geometry.base import BaseGeometry

from gis_tool.db_utils import upsert_mongodb_feature

logger = logging.getLogger("gis_tool.data_utils")

SCHEMA_FIELDS = ["Name", "Date", "PSI", "Material", "geometry"]


def make_feature(
    name: str,
    date: Union[str, pd.Timestamp],
    psi: float,
    material: str,
    geometry: BaseGeometry,
    crs: str
) -> gpd.GeoDataFrame:
    """
    Create a GeoDataFrame containing a single pipeline feature with specified attributes.

    Normalizes the 'material' field to lowercase for consistency.
    The feature adheres to a predefined schema for compatibility.

    Args:
        name (str): The name or ID of the pipeline feature.
        date (Union[str, pd.Timestamp]): The date associated with the feature.
        psi (float): The pressure value for the pipeline.
        material (str): The pipeline material (case-insensitive).
        geometry (BaseGeometry): A Shapely geometry object (typically Point).
        crs (str): The coordinate reference system (e.g., "EPSG:4326").

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing one feature with specified CRS.
    """
    logger.debug(
        f"Creating feature: name={name}, date={date}, psi={psi}, material={material}, "
        f"geometry={geometry.wkt}, crs={crs}"
    )

    data = {
        SCHEMA_FIELDS[0]: [name],
        SCHEMA_FIELDS[1]: [date],
        SCHEMA_FIELDS[2]: [psi],
        SCHEMA_FIELDS[3]: [material.lower()],
        SCHEMA_FIELDS[4]: [geometry]
    }

    return gpd.GeoDataFrame(data, crs=crs)


def create_and_upsert_feature(
    name: str,
    date: Union[str, pd.Timestamp],
    psi: float,
    material: str,
    geometry: BaseGeometry,
    spatial_reference: str,
    gas_lines_gdf: gpd.GeoDataFrame,
    gas_lines_collection: Optional[Collection],
    use_mongodb: bool
) -> gpd.GeoDataFrame:
    """
    Create and insert a new pipeline feature, optionally upserting into MongoDB.

    Supports various geometry types but only upserts Points to MongoDB.
    Aligns schema and geometry columns before returning updated GeoDataFrame.

    Args:
        name (str): Name/ID of the pipeline feature.
        date (Union[str, pd.Timestamp]): Associated timestamp.
        psi (float): Pressure reading.
        material (str): Pipeline material.
        geometry (BaseGeometry): Feature geometry (Point, LineString, etc.).
        spatial_reference (str): CRS string for spatial alignment.
        gas_lines_gdf (gpd.GeoDataFrame): Existing pipeline data.
        gas_lines_collection (Optional[Collection]): MongoDB collection handle.
        use_mongodb (bool): Flag to control whether to write to MongoDB.

    Returns:
        gpd.GeoDataFrame: Updated GeoDataFrame including the new feature.
    """
    new_feature = make_feature(name, date, psi, material, geometry, spatial_reference)

    if use_mongodb and gas_lines_collection is not None:
        if geometry.geom_type != "Point":
            logger.warning(
                f"MongoDB only supports Point geometry directly. "
                f"Feature '{name}' has type {geometry.geom_type}."
            )
        logger.debug(f"Inserting/updating feature in MongoDB: {name}")
        upsert_mongodb_feature(
            gas_lines_collection, name, date, psi, material, geometry
        )

    # Align new feature schema with existing data columns
    new_feature = new_feature.reindex(columns=gas_lines_gdf.columns)

    if 'geometry' in new_feature.columns:
        new_feature.set_geometry('geometry', inplace=True)

    logger.debug(f"Adding new feature: {name}")

    if not new_feature.empty:
        # Separate geometry and attributes for existing and new data, drop empty attribute columns
        non_geom_cols = [col for col in gas_lines_gdf.columns if col != 'geometry']
        gas_lines_clean = gpd.GeoDataFrame(
            gas_lines_gdf['geometry'], crs=gas_lines_gdf.crs
        ).join(gas_lines_gdf[non_geom_cols].dropna(axis=1, how='all'))

        non_geom_cols_nf = [col for col in new_feature.columns if col != 'geometry']
        new_feature_clean = gpd.GeoDataFrame(
            new_feature['geometry'], crs=new_feature.crs
        ).join(new_feature[non_geom_cols_nf].dropna(axis=1, how='all'))

        combined_gdf = pd.concat([gas_lines_clean, new_feature_clean], ignore_index=True)
        return gpd.GeoDataFrame(combined_gdf, crs=gas_lines_gdf.crs)

    logger.debug(f"New feature for {name} is empty, skipping concat.")
    return gas_lines_gdf
