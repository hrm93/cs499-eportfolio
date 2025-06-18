"""
db_utils.py

MongoDB utility functions for spatial data pipelines.

This module manages MongoDB connectivity, schema enforcement, spatial indexing,
and feature insertion/updating. It includes geometry validation, reprojection,
and compliance with 2dsphere indexing standards.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import warnings
from typing import Dict, Optional, Union

import geopandas as gpd
import pandas as pd
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, PyMongoError
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry

from gis_tool.config import MONGODB_URI, DB_NAME, DEFAULT_CRS
from gis_tool.geometry_cleaning import (
    fix_geometry,
    simplify_geometry,
    is_finite_geometry,
)

logger = logging.getLogger("gis_tool.db_utils")

# MongoDB schema used to validate spatial feature documents
spatial_feature_schema = {
    "bsonType": "object",
    "required": ["name", "geometry", "date", "psi", "material"],
    "properties": {
        "name": {"bsonType": "string"},
        "geometry": {
            "bsonType": "object",
            "required": ["type", "coordinates"],
            "description": "GeoJSON geometry",
            "properties": {
                "type": {"enum": ["Point", "LineString", "Polygon"]},
                "coordinates": {"bsonType": "array"}
            },
        },
        "date": {"bsonType": "string"},
        "psi": {"bsonType": "double"},
        "material": {"bsonType": "string"},
    },
}


def ensure_spatial_index(collection: Collection) -> None:
    """
    Ensure a 2dsphere geospatial index exists on the 'geometry' field.

    This index enables MongoDB geospatial queries. If it does not exist, it is created.
    """
    try:
        indexes = collection.index_information()
        if not any(
            len(index.get('key', [])) == 1 and
            index['key'][0][0] == 'geometry' and
            index['key'][0][1] == '2dsphere'
            for index in indexes.values()
        ):
            collection.create_index([("geometry", "2dsphere")], name="geometry_2dsphere")
            logger.info("Created 2dsphere index on 'geometry' field.")
        else:
            logger.info("2dsphere index already exists on 'geometry' field.")
    except PyMongoError as e:
        logger.error(f"Failed to ensure spatial index: {e}")
        raise


def ensure_collection_schema(
    db: Database, collection_name: str, schema: dict
) -> None:
    """
    Ensure the MongoDB collection uses a JSON Schema validator.

    Creates the collection with the validator if it does not exist.
    If it exists, modifies the validator to enforce schema constraints.
    """
    logger.info(f"Ensuring JSON Schema for collection: {collection_name}")
    try:
        db.create_collection(collection_name, validator={"$jsonSchema": schema})
        logger.debug(f"Created collection '{collection_name}' with schema.")
    except Exception as e:
        if "already exists" in str(e):
            try:
                db.command({
                    "collMod": collection_name,
                    "validator": {"$jsonSchema": schema},
                    "validationLevel": "strict",
                })
                logger.info(f"Updated schema validator for collection '{collection_name}'.")
            except PyMongoError as cmd_err:
                logger.error(f"Failed to update schema for '{collection_name}': {cmd_err}")
                raise
        else:
            logger.error(f"Failed to ensure schema for '{collection_name}': {e}")
            raise


def connect_to_mongodb(
    uri: str = MONGODB_URI, db_name: str = DB_NAME
) -> Database:
    """
    Establish a connection to MongoDB and return the database instance.

    Raises:
        ConnectionFailure: if unable to connect to MongoDB.
        Exception: for any unexpected connection error.
    """
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")  # Test connection
        logger.info("Connected to MongoDB successfully.")
        return client[db_name]
    except ConnectionFailure as e:
        msg = f"Could not connect to MongoDB at {uri}: {e}"
        warnings.warn(msg, UserWarning)
        logger.error(msg)
        raise
    except Exception as e:
        msg = f"Unexpected error connecting to MongoDB at {uri}: {e}"
        warnings.warn(msg, UserWarning)
        logger.error(msg)
        raise


def reproject_to_wgs84(geometry: BaseGeometry, input_crs: str = "EPSG:3857") -> Optional[Dict]:
    """
    Reproject a Shapely geometry to WGS 84 (EPSG:4326) and return a GeoJSON-like dictionary.

    Applies geometry fixing, simplification, and CRS transformation.

    Returns:
        dict: GeoJSON geometry dictionary if valid, else None.
    """
    if geometry is None:
        logger.warning("Input geometry is None, skipping reprojection.")
        return None

    if not input_crs:
        raise ValueError("Input CRS is not defined for reprojection.")

    fixed_geom = fix_geometry(geometry)
    if fixed_geom is None:
        logger.warning("Geometry could not be fixed, skipping reprojection.")
        return None

    simplified_geom = simplify_geometry(fixed_geom)

    if input_crs == "EPSG:4326":
        geom_to_check = simplified_geom
    else:
        try:
            gdf = gpd.GeoDataFrame(index=[0], geometry=[simplified_geom], crs=input_crs)
            gdf_wgs84 = gdf.to_crs("EPSG:4326")
            geom_to_check = gdf_wgs84.iloc[0].geometry
        except Exception as e:
            logger.error(f"Failed to reproject geometry: {e}")
            return None

    geojson_geom = mapping(geom_to_check)

    if not is_finite_geometry(geojson_geom):
        logger.warning("Geometry contains non-finite coordinates after reprojection and cleaning.")
        return None

    return geojson_geom


def upsert_mongodb_feature(
    collection: Collection,
    name: str,
    date: Union[str, pd.Timestamp],
    psi: float,
    material: str,
    geometry: BaseGeometry,
    input_crs: str = DEFAULT_CRS,
) -> None:
    """
    Insert or update a pipeline feature document in MongoDB.

    Handles reprojection, schema enforcement, and safe upserting.
    Logs and skips insertion for invalid or unsupported geometries.
    """
    if not isinstance(name, str) or not name.strip():
        msg = "Invalid 'name': must be non-empty string."
        warnings.warn(msg, UserWarning)
        logger.error(msg)
        raise ValueError(msg)

    if geometry.geom_type not in {"Point", "LineString", "Polygon"}:
        msg = f"Unsupported geometry type: {geometry.geom_type}"
        warnings.warn(msg, UserWarning)
        logger.error(msg)
        raise ValueError(msg)

    if isinstance(date, pd.Timestamp):
        date = date.isoformat()

    psi = float(psi)

    geometry_geojson = reproject_to_wgs84(geometry, input_crs)
    logger.debug(f"[MongoDB Upsert] Feature '{name}' reprojection complete using input_crs='{input_crs}'.")

    if geometry_geojson is None:
        msg = f"Skipping upsert for '{name}': invalid or non-finite geometry after cleaning/reprojection."
        warnings.warn(msg, UserWarning)
        logger.warning(msg)
        return

    if not is_finite_geometry(geometry_geojson):
        msg = f"Skipping upsert for '{name}': geometry contains non-finite coordinates."
        warnings.warn(msg, UserWarning)
        logger.warning(msg)
        return

    feature_doc = {
        "name": name,
        "date": date,
        "psi": psi,
        "material": material,
        "geometry": geometry_geojson,
    }

    try:
        existing = collection.find_one({"name": name, "geometry": geometry_geojson})
    except PyMongoError as e:
        msg = f"MongoDB query failed for '{name}': {e}"
        warnings.warn(msg, UserWarning)
        logger.error(msg)
        raise

    if existing:
        # Only update changed fields
        changes = {
            k: v for k, v in {"date": date, "psi": psi, "material": material}.items()
            if existing.get(k) != v
        }
        if changes:
            try:
                collection.update_one({"_id": existing["_id"]}, {"$set": changes})
                logger.info(f"Updated MongoDB record for '{name}' with changes: {changes}")
            except PyMongoError as e:
                msg = f"MongoDB update failed for '{name}': {e}"
                warnings.warn(msg, UserWarning)
                logger.error(msg)
                raise
        else:
            logger.info(f"No changes for MongoDB record '{name}', skipping update.")
    else:
        try:
            collection.insert_one(feature_doc)
            logger.info(f"Inserted new MongoDB record for '{name}'.")
        except PyMongoError as e:
            msg = f"MongoDB insert failed for '{name}': {e}"
            warnings.warn(msg, UserWarning)
            logger.error(msg)
            raise
