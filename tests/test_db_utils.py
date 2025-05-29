import logging
import pytest
from unittest.mock import MagicMock
from pymongo.errors import ConnectionFailure
from shapely.geometry import Point

from gis_tool.db_utils import connect_to_mongodb, upsert_mongodb_feature

logger = logging.getLogger("gis_tool")


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
    Test updating an existing MongoDB feature.

    Mocks existing document with different attributes, and
    verifies update_one is called with correct filter and update.
    """
    logger.info("Testing upsert: update existing MongoDB feature.")

    mock_collection = MagicMock()
    geometry = Point(3, 4)

    existing_doc = {
        "_id": "12345",
        "name": "Line B",
        "date": "2023-12-31",
        "psi": 100.0,
        "material": "Iron",
        "geometry": geometry.wkt  # Simplified geometry stored as WKT
    }

    monkeypatch.setattr(
        "gis_tool.db_utils.simplify_geometry",
        lambda g: g.wkt  # match existing_doc format
    )

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

    logger.info("Update existing feature test passed.")


def test_upsert_mongodb_feature_no_update(monkeypatch):
    """
    Test that no update or insert occurs if existing document matches
    all provided values.

    Verifies that neither insert_one nor update_one is called.
    """
    logger.info("Testing upsert: no operation when data matches existing doc.")

    mock_collection = MagicMock()
    geometry = Point(3, 4)

    existing_doc = {
        "_id": "99999",
        "name": "Line C",
        "date": "2024-01-01",
        "psi": 250.0,
        "material": "Steel",
        "geometry": geometry.wkt
    }

    monkeypatch.setattr(
        "gis_tool.db_utils.simplify_geometry",
        lambda g: g.wkt
    )

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
