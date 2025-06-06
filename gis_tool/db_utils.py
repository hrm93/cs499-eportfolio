# db_utils.py

import logging
import warnings
import pandas as pd
from typing import Union

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, PyMongoError

from shapely.geometry import Point

from gis_tool.geometry_cleaning import simplify_geometry
from gis_tool.config import MONGODB_URI, DB_NAME

logger = logging.getLogger("gis_tool")


# Define the JSON Schema for the 'features' collection.
# This schema enforces the structure of spatial features stored in MongoDB.
# It requires fields: name, geometry, date, psi, and material.
# The 'geometry' field is expected to contain a valid GeoJSON object.

spatial_feature_schema = {
    "bsonType": "object",
    "required": ["name", "geometry", "date", "psi", "material"],
    "properties": {
        "name": {"bsonType": "string"},
        "geometry": {
            "bsonType": "object",
            "description": "GeoJSON geometry"
        },
        "date": {"bsonType": "string"},
        "psi": {"bsonType": "double"},
        "material": {"bsonType": "string"}
    }
}
def ensure_spatial_index(collection: Collection) -> None:
    """
    Ensure that a 2dsphere index exists on the 'geometry' field.
    """
    indexes = collection.index_information()
    if 'geometry_2dsphere' not in indexes:
        collection.create_index([('geometry', '2dsphere')])
        logger.info("Created 2dsphere index on 'geometry' field.")
    else:
        logger.info("2dsphere index already exists on 'geometry' field.")


def ensure_collection_schema(db, collection_name, schema):
    """
    Ensure that the MongoDB collection has a JSON Schema validator.

    If the collection does not exist, it is created with the specified schema.
    If the collection already exists, the schema validator is updated to match the provided schema.

    Args:
        db (Database): The MongoDB database object.
        collection_name (str): The name of the collection to ensure the schema for.
        schema (dict): The JSON Schema to apply as a validator.

    Raises:
        Exception: If an unexpected error occurs during creation or update.
    """
    logger.info(f"Ensuring JSON Schema for collection: {collection_name}")
    try:
        # Attempt to create the collection with the JSON Schema validator
        db.create_collection(
            collection_name,
            validator={"$jsonSchema": schema}
        )
        logger.debug(f"Created new collection '{collection_name}' with schema: {schema}")
    except Exception as e:
        if "already exists" in str(e):
            # If the collection already exists, update the validator using collMod
            logger.debug(f"Collection '{collection_name}' already exists; updating schema validator.")
            db.command({
                "collMod": collection_name,
                "validator": {"$jsonSchema": schema},
                "validationLevel": "strict"
            })
            logger.info(f"Updated schema validator for collection '{collection_name}'.")
        else:
            # If another error occurs, log and re-raise
            logger.error(f"Failed to ensure schema for collection '{collection_name}': {e}")
            raise e


def connect_to_mongodb(uri: str = MONGODB_URI, db_name: str = DB_NAME) -> Database:
    """
    Establish a connection to a MongoDB database.

    Args:
        uri (str): MongoDB connection URI.
        db_name (str): Name of the database to connect.

    Returns:
        pymongo.database.Database: Connected MongoDB database instance.

    Raises:
        ConnectionFailure: If the connection to MongoDB fails.
    """
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')  # Test the connection
        logger.info("Connected to MongoDB successfully.")
        return client[db_name]
    except ConnectionFailure as e:
        warning_msg = f"Could not connect to MongoDB at {uri}: {e}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise
    except Exception as e:
        warning_msg = f"Unexpected error connecting to MongoDB at {uri}: {e}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise


def upsert_mongodb_feature(
    collection: Collection,
    name: str,
    date: Union[str, pd.Timestamp],
    psi: float,
    material: str,
    geometry: Point
) -> None:
    """
    Insert or update a gas line feature in MongoDB.

    Args:
        collection (Collection): MongoDB collection to update/insert into.
        name (str): Name of the gas line feature.
        date: Date associated with the feature (can be string or datetime).
        psi (float): Pressure specification.
        material (str): Material type.
        geometry (Point): Shapely Point geometry object.

    Raises:
        PyMongoError: On errors interacting with MongoDB.
        ValueError: If input parameters are invalid.
    """
    if not isinstance(name, str) or not name.strip():
        warning_msg = "Invalid 'name' parameter: must be a non-empty string."
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise ValueError(warning_msg)

    if not isinstance(geometry, Point):
        warning_msg = f"Invalid geometry type: expected shapely.geometry.Point, got {type(geometry)}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise ValueError(warning_msg)

    feature_doc = {
        'name': name,
        'date': date,
        'psi': psi,
        'material': material,
        'geometry': simplify_geometry(geometry)  # Use simplified geometry
    }

    try:
        existing = collection.find_one({'name': name, 'geometry': feature_doc['geometry']})
    except PyMongoError as e:
        warning_msg = f"Failed to query MongoDB for existing feature '{name}': {e}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise

    if existing:
        # Check if any fields changed before updating
        changes = {}
        if existing.get('date') != date:
            changes['date'] = date
        if existing.get('psi') != psi:
            changes['psi'] = psi
        if existing.get('material') != material:
            changes['material'] = material

        if changes:
            try:
                collection.update_one({'_id': existing['_id']}, {'$set': changes})
                logger.info(f"Updated MongoDB record for '{name}' with changes: {changes}")
            except PyMongoError as e:
                warning_msg = f"Failed to update MongoDB record for '{name}': {e}"
                warnings.warn(warning_msg, UserWarning)
                logger.error(warning_msg)
                raise
        else:
            logger.info(f"No changes detected for MongoDB record '{name}', skipping update.")
    else:
        try:
            collection.insert_one(feature_doc)
            logger.info(f"Inserted new MongoDB record for '{name}'.")
        except PyMongoError as e:
            warning_msg = f"Failed to insert new MongoDB record for '{name}': {e}"
            warnings.warn(warning_msg, UserWarning)
            logger.error(warning_msg)
            raise
