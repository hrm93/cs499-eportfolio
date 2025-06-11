import logging
import warnings
from typing import Union

import geopandas as gpd
import pandas as pd
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, PyMongoError

from shapely.geometry.base import BaseGeometry
from shapely.geometry import mapping

from gis_tool.config import MONGODB_URI, DB_NAME
from gis_tool.spatial_utils import is_finite_geometry

logger = logging.getLogger("gis_tool")

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
    """Ensure a 2dsphere index exists on the 'geometry' field."""
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


def ensure_collection_schema(
    db: Database, collection_name: str, schema: dict
) -> None:
    """
    Ensure the MongoDB collection has a JSON Schema validator.

    Creates the collection with the validator if it doesn't exist,
    otherwise updates the validator using collMod.
    """
    logger.info(f"Ensuring JSON Schema for collection: {collection_name}")
    try:
        db.create_collection(collection_name, validator={"$jsonSchema": schema})
        logger.debug(f"Created collection '{collection_name}' with schema.")
    except Exception as e:
        if "already exists" in str(e):
            db.command(
                {
                    "collMod": collection_name,
                    "validator": {"$jsonSchema": schema},
                    "validationLevel": "strict",
                }
            )
            logger.info(f"Updated schema validator for collection '{collection_name}'.")
        else:
            logger.error(f"Failed to ensure schema for '{collection_name}': {e}")
            raise


def connect_to_mongodb(
    uri: str = MONGODB_URI, db_name: str = DB_NAME
) -> Database:
    """
    Connect to MongoDB and return the database instance.
    Raises ConnectionFailure or other exceptions on failure.
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


def reproject_to_wgs84(geometry: BaseGeometry, input_crs: str = "EPSG:3857") -> dict:
    """
    Reproject shapely geometry to EPSG:4326 and return GeoJSON dict.
    """
    gdf = gpd.GeoDataFrame(geometry=[geometry], crs=input_crs)
    gdf_wgs84 = gdf.to_crs("EPSG:4326")
    return mapping(gdf_wgs84.iloc[0].geometry)


def upsert_mongodb_feature(
    collection: Collection,
    name: str,
    date: Union[str, pd.Timestamp],
    psi: float,
    material: str,
    geometry: BaseGeometry,
    input_crs: str = "EPSG:3857",
) -> None:
    """
    Insert or update a gas line feature in MongoDB.

    Validates inputs, reprojects geometry, and performs upsert.
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
        changes = {
            k: v
            for k, v in {"date": date, "psi": psi, "material": material}.items()
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
