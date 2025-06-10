# Tests for db_utils.py

import logging
from unittest.mock import MagicMock

import pytest
from pymongo.errors import ConnectionFailure
from shapely.geometry import Point, mapping

from gis_tool.db_utils import (
    ensure_spatial_index,
    connect_to_mongodb,
    upsert_mongodb_feature,
    ensure_collection_schema,
    spatial_feature_schema,
)

logger = logging.getLogger("gis_tool")


# ------------------------
# Tests for ensure_spatial_index
# ------------------------

def test_ensure_spatial_index_creates_index(monkeypatch, caplog):
    """
    Test that ensure_spatial_index creates the 2dsphere index when missing.
    """
    logger.info("Testing ensure_spatial_index: creates index if missing.")

    mock_collection = MagicMock()
    mock_collection.index_information.return_value = {}

    ensure_spatial_index(mock_collection)

    mock_collection.create_index.assert_called_once_with([("geometry", "2dsphere")])

    assert "Created 2dsphere index" in caplog.text
    logger.info("ensure_spatial_index creates index test passed.")


def test_ensure_spatial_index_skips_existing_index(monkeypatch, caplog):
    """
    Test that ensure_spatial_index does not recreate an index if it already exists.
    """
    logger.info("Testing ensure_spatial_index: skips creation if index exists.")

    mock_collection = MagicMock()
    mock_collection.index_information.return_value = {"geometry_2dsphere": {}}

    ensure_spatial_index(mock_collection)

    mock_collection.create_index.assert_not_called()

    assert "2dsphere index already exists" in caplog.text
    logger.info("ensure_spatial_index skips existing index test passed.")

# ------------------------
# Tests for ensure_collection_schema
# ------------------------

def test_ensure_collection_schema_creates_new_collection(monkeypatch, caplog):
    """
    Test that ensure_collection_schema creates a new collection with the JSON Schema
    if the collection does not exist.
    """
    logger.info("Testing ensure_collection_schema: create new collection with schema.")

    mock_db = MagicMock()
    # Simulate create_collection not raising any exception (collection does not exist)
    mock_db.create_collection.return_value = None

    ensure_collection_schema(mock_db, "features", spatial_feature_schema)

    mock_db.create_collection.assert_called_once_with(
        "features",
        validator={"$jsonSchema": spatial_feature_schema}
    )
    assert "Created new collection" in caplog.text or "Ensuring JSON Schema" in caplog.text

    logger.info("ensure_collection_schema creation test passed.")


def test_ensure_collection_schema_updates_existing_collection(monkeypatch, caplog):
    """
    Test that ensure_collection_schema updates the validator of an existing collection
    using the 'collMod' command when the collection already exists.
    """
    logger.info("Testing ensure_collection_schema: update validator if collection exists.")

    mock_db = MagicMock()

    # Simulate create_collection raising an error indicating that collection exists
    def create_collection_side_effect(*args, **kwargs):
        raise Exception("Collection already exists")

    mock_db.create_collection.side_effect = create_collection_side_effect

    # Mock the db.command call for collMod
    mock_db.command.return_value = {"ok": 1}

    ensure_collection_schema(mock_db, "features", spatial_feature_schema)

    mock_db.command.assert_called_once_with({
        "collMod": "features",
        "validator": {"$jsonSchema": spatial_feature_schema},
        "validationLevel": "strict"
    })

    assert "Updated schema validator for collection" in caplog.text
    logger.info("ensure_collection_schema update test passed.")


def test_ensure_collection_schema_raises_unexpected_error(monkeypatch, caplog):
    """
    Test that ensure_collection_schema raises an exception if an unexpected error occurs
    during collection creation or update.
    """
    logger.info("Testing ensure_collection_schema: handles unexpected error gracefully.")

    mock_db = MagicMock()

    # Simulate an unexpected error other than "already exists"
    def create_collection_side_effect(*args, **kwargs):
        raise Exception("Unexpected error")

    mock_db.create_collection.side_effect = create_collection_side_effect

    with caplog.at_level("ERROR", logger="gis_tool"):
        with pytest.raises(Exception, match="Unexpected error"):
            ensure_collection_schema(mock_db, "features", spatial_feature_schema)

    assert "Failed to ensure schema" in caplog.text
    logger.info("ensure_collection_schema error handling test passed.")


# ------------------------
# Tests for connect_to_mongodb
# ------------------------

def test_connect_to_mongodb_success(monkeypatch):
    """
    Test successful connection to MongoDB.

    Uses monkeypatch to mock MongoClient and simulate a successful
    connection and database retrieval.
    """
    logger.info("Testing successful MongoDB connection.")

    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db

    class MockMongoClient:
        def __init__(self, *args, **kwargs):
            pass

        @property
        def admin(self):
            return MagicMock(command=MagicMock(return_value={"ok": 1}))

        def __getitem__(self, name):
            return mock_db

    monkeypatch.setattr("gis_tool.db_utils.MongoClient", MockMongoClient)

    result = connect_to_mongodb("mongodb://localhost", "test_db")
    logger.debug(f"MongoDB connection returned db object: {result}")
    assert result == mock_db

    logger.info("MongoDB connection success test passed.")


def test_connect_to_mongodb_failure(monkeypatch, caplog):
    """
    Test MongoDB connection failure.

    Monkeypatch MongoClient to raise ConnectionFailure,
    and verify that the function raises and logs an error.
    """
    logger.info("Testing MongoDB connection failure scenario.")

    def fail_client(*args, **kwargs):
        raise ConnectionFailure("Simulated connection failure")

    monkeypatch.setattr("gis_tool.db_utils.MongoClient", fail_client)

    with caplog.at_level("ERROR", logger="gis_tool"):
        with pytest.raises(ConnectionFailure, match="Simulated connection failure"):
            connect_to_mongodb("mongodb://baduri", "test_db")

    logger.debug(f"Captured logs: {caplog.text}")
    assert "Could not connect to MongoDB" in caplog.text

    logger.info("MongoDB connection failure test passed.")


# ------------------------
# Tests for upsert_mongodb_feature
# ------------------------

def test_upsert_mongodb_feature_insert(monkeypatch):
    """
    Test inserting a new feature into MongoDB collection.

    Verifies that insert_one is called with correct document fields
    when no existing document matches.
    """
    logger.info("Testing upsert: insert new MongoDB feature.")

    mock_collection = MagicMock()
    mock_collection.find_one.return_value = None

    geometry = Point(1, 2)

    upsert_mongodb_feature(
        collection=mock_collection,
        name="Line A",
        date="2024-01-01",
        psi=250.0,
        material="Steel",
        geometry=geometry
    )

    mock_collection.insert_one.assert_called_once()
    inserted_doc = mock_collection.insert_one.call_args[0][0]
    logger.debug(f"Inserted document: {inserted_doc}")

    assert inserted_doc["name"] == "Line A"
    assert inserted_doc["material"] == "Steel"
    assert "geometry" in inserted_doc

    logger.info("Insert new feature test passed.")


def test_upsert_mongodb_feature_update(monkeypatch):
    """
    Test updating an existing MongoDB feature with GeoJSON geometry in WGS84.
    """
    logger.info("Testing upsert: update existing MongoDB feature.")

    mock_collection = MagicMock()
    geometry = Point(3, 4)

    # Manually create WGS84 GeoJSON geometry for the existing doc (simulate reprojection)
    geometry_geojson = mapping(geometry)

    existing_doc = {
        "_id": "12345",
        "name": "Line B",
        "date": "2023-12-31",
        "psi": 100.0,
        "material": "Iron",
        "geometry": geometry_geojson
    }

    mock_collection.find_one.return_value = existing_doc

    upsert_mongodb_feature(
        collection=mock_collection,
        name="Line B",
        date="2024-01-01",
        psi=120.0,
        material="Steel",
        geometry=geometry
    )

    mock_collection.update_one.assert_called_once()
    update_call = mock_collection.update_one.call_args[0]
    logger.debug(f"Update filter: {update_call[0]}, update: {update_call[1]}")

    assert update_call[0] == {"_id": "12345"}
    assert "$set" in update_call[1]
    assert update_call[1]["$set"]["date"] == "2024-01-01"
    assert update_call[1]["$set"]["psi"] == 120.0
    assert update_call[1]["$set"]["material"] == "Steel"

    logger.info("Update existing feature test passed.")


def test_upsert_mongodb_feature_no_update(monkeypatch):
    """
    Test that no update or insert occurs if existing document matches
    all provided values, using GeoJSON geometry.
    """
    logger.info("Testing upsert: no operation when data matches existing doc.")

    mock_collection = MagicMock()
    geometry = Point(3, 4)

    geometry_geojson = mapping(geometry)

    existing_doc = {
        "_id": "99999",
        "name": "Line C",
        "date": "2024-01-01",
        "psi": 250.0,
        "material": "Steel",
        "geometry": geometry_geojson
    }

    mock_collection.find_one.return_value = existing_doc

    upsert_mongodb_feature(
        collection=mock_collection,
        name="Line C",
        date="2024-01-01",
        psi=250.0,
        material="Steel",
        geometry=geometry
    )

    mock_collection.update_one.assert_not_called()
    mock_collection.insert_one.assert_not_called()

    logger.info("No update or insert test passed when data is unchanged.")
