import logging
from typing import Union, Optional

import geopandas as gpd
import pandas as pd
from pymongo.collection import Collection
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

from gis_tool.db_utils import upsert_mongodb_feature

logger = logging.getLogger("gis_tool")


# Note: 'material' field is normalized to lowercase for consistency.
# Other string fields like 'name' retain original casing.
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

      The feature fields correspond to the predefined SCHEMA_FIELDS.
      The material string is normalized to lowercase.

      Args:
          name (str): The name/ID of the pipeline feature.
          date (Union[str, pd.Timestamp]): The date associated with the feature.
          psi (float): The pressure measurement for the pipeline.
          material (str): The material of the pipeline (case-insensitive).
          geometry (Point): The geometric location as a Shapely Point.
          crs (str): The coordinate reference system string (e.g., "EPSG:4326").

      Returns:
          gpd.GeoDataFrame: A GeoDataFrame with one row representing the feature,
          using the provided CRS.
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
    Create and insert a new feature supporting multiple geometry types (Point, LineString, Polygon).
    Optionally upserts into MongoDB if enabled.
    """
    new_feature = make_feature(name, date, psi, material, geometry, spatial_reference)

    if use_mongodb and gas_lines_collection is not None:
        if geometry.geom_type != "Point":
            logger.warning(
                f"MongoDB only supports Point geometry directly. Feature '{name}' has type {geometry.geom_type}."
            )
        logger.debug(f"Inserting/updating feature in MongoDB: {name}")
        upsert_mongodb_feature(
            gas_lines_collection, name, date, psi, material, geometry
        )
    # Align schema
    new_feature = new_feature.reindex(columns=gas_lines_gdf.columns)

    if 'geometry' in new_feature.columns:
        new_feature.set_geometry('geometry', inplace=True)

    logger.debug(f"Adding new feature: {name}")
    # Concatenate and ensure the result is a GeoDataFrame
    combined_gdf = gpd.GeoDataFrame(
        pd.concat([gas_lines_gdf, new_feature], ignore_index=True),
        crs=gas_lines_gdf.crs
    )

    return combined_gdf
