import logging
from unittest.mock import MagicMock, patch
from typing import Any

import pytest

from pymongo.errors import ConnectionFailure
from shapely.geometry import Point, mapping, shape

from shapely.geometry.base import BaseGeometry

from gis_tool.db_utils import (
    ensure_spatial_index,
    connect_to_mongodb,
    upsert_mongodb_feature,
    ensure_collection_schema,
    spatial_feature_schema,
    reproject_to_wgs84,
)

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs during testing


# ------------------------
# Tests for ensure_spatial_index
# ------------------------

@pytest.mark.parametrize(
    "index_info, should_create, expected_log",
    [
        ({}, True, "Created 2dsphere index"),
        (
            {"geometry_2dsphere": {"key": [("geometry", "2dsphere")]}},
            False,
            "2dsphere index already exists"
        )
    ],
)
def test_ensure_spatial_index_behavior(
    index_info: dict[str, Any], should_create: bool, expected_log: str, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Test ensure_spatial_index creates a 2dsphere index if missing or skips creation if present.

    Args:
        index_info: The simulated index information returned by the collection.
        should_create: Whether the index is expected to be created.
        expected_log: The expected log message indicating action taken.
        caplog: Pytest fixture for capturing logs.
    """
    logger.info(f"Starting test_ensure_spatial_index_behavior with index_info={index_info}")

    mock_collection = MagicMock()
    # Mock index_information to return provided index_info
    mock_collection.index_information.return_value = index_info

    # Run ensure_spatial_index with INFO log capture
    with caplog.at_level(logging.INFO, logger="gis_tool"):
        ensure_spatial_index(mock_collection)

    # Verify if create_index was called or not based on expectation
    if should_create:
        mock_collection.create_index.assert_called_once_with(
            [("geometry", "2dsphere")], name="geometry_2dsphere"
        )
        logger.info("Index creation verified")
    else:
        mock_collection.create_index.assert_not_called()
        logger.info("Index creation skipped as expected")

    # Confirm expected log message appears
    assert expected_log in caplog.text
    logger.info("Expected log message found in caplog")


# ------------------------
# Tests for ensure_collection_schema
# ------------------------

def test_ensure_collection_schema_creates_new_collection(caplog: pytest.LogCaptureFixture) -> None:
    """
    Test that ensure_collection_schema creates a new collection with the given JSON schema
    validator when the collection does not already exist.

    Args:
        caplog: Pytest fixture for capturing logs.
    """
    logger.info("Starting test: ensure_collection_schema_creates_new_collection")

    mock_db = MagicMock()
    # Simulate successful collection creation
    mock_db.create_collection.return_value = None

    # Run function with INFO logging
    with caplog.at_level(logging.INFO, logger="gis_tool"):
        ensure_collection_schema(mock_db, "features", spatial_feature_schema)

    # Validate create_collection called with correct args
    mock_db.create_collection.assert_called_once_with(
        "features",
        validator={"$jsonSchema": spatial_feature_schema},
    )

    # Check log messages for creation or validation notice
    assert any(
        phrase in caplog.text for phrase in ["Created new collection", "Ensuring JSON Schema"]
    )

    logger.info("Completed test: ensure_collection_schema_creates_new_collection")


def test_ensure_collection_schema_updates_existing_collection(caplog: pytest.LogCaptureFixture) -> None:
    """
    Test that ensure_collection_schema updates the validator of an existing collection
    via the 'collMod' command if the collection already exists.

    Args:
        caplog: Pytest fixture for capturing logs.
    """
    logger.info("Starting test: ensure_collection_schema_updates_existing_collection")

    mock_db = MagicMock()

    # Simulate exception indicating collection already exists
    def create_collection_side_effect(*args, **kwargs) -> None:
        raise Exception("Collection already exists")

    mock_db.create_collection.side_effect = create_collection_side_effect
    mock_db.command.return_value = {"ok": 1}  # Simulate successful collMod command

    with caplog.at_level(logging.INFO, logger="gis_tool"):
        ensure_collection_schema(mock_db, "features", spatial_feature_schema)

    # Verify the collMod command was issued with correct parameters
    mock_db.command.assert_called_once_with(
        {
            "collMod": "features",
            "validator": {"$jsonSchema": spatial_feature_schema},
            "validationLevel": "strict",
        }
    )
    # Confirm update log presence
    assert "Updated schema validator for collection" in caplog.text

    logger.info("Completed test: ensure_collection_schema_updates_existing_collection")


def test_ensure_collection_schema_raises_unexpected_error(caplog: pytest.LogCaptureFixture) -> None:
    """
    Test that ensure_collection_schema raises an exception and logs an error
    if an unexpected exception occurs during collection creation or update.

    Args:
        caplog: Pytest fixture for capturing logs.
    """
    logger.info("Starting test: ensure_collection_schema_raises_unexpected_error")

    mock_db = MagicMock()

    # Simulate unexpected error during create_collection
    def create_collection_side_effect(*args, **kwargs) -> None:
        logger.error("Simulating unexpected error in create_collection")
        raise Exception("Unexpected error")

    mock_db.create_collection.side_effect = create_collection_side_effect

    with caplog.at_level(logging.ERROR, logger="gis_tool"):
        with pytest.raises(Exception, match="Unexpected error"):
            ensure_collection_schema(mock_db, "features", spatial_feature_schema)

    # Check error log message
    assert "Failed to ensure schema" in caplog.text

    logger.info("Completed test: ensure_collection_schema_raises_unexpected_error")


# ------------------------
# Tests for connect_to_mongodb
# ------------------------

def test_connect_to_mongodb_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test successful connection to MongoDB, returning the database instance.

    Args:
        monkeypatch: Pytest fixture to patch MongoClient.
    """
    logger.info("Starting test: connect_to_mongodb_success")

    mock_db = MagicMock()

    class MockMongoClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        @property
        def admin(self) -> MagicMock:
            # Simulate successful ping to admin database
            return MagicMock(command=MagicMock(return_value={"ok": 1}))

        def __getitem__(self, name: str) -> MagicMock:
            # Return mocked database instance
            return mock_db

    # Patch MongoClient to use the mock client
    monkeypatch.setattr("gis_tool.db_utils.MongoClient", MockMongoClient)

    # Call function under test
    result = connect_to_mongodb("mongodb://localhost", "test_db")

    # Assert returned database is mock_db
    assert result == mock_db

    logger.info("Completed test: connect_to_mongodb_success")


def test_connect_to_mongodb_failure(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """
    Test that connect_to_mongodb raises ConnectionFailure and logs an error
    when MongoDB connection fails.

    Args:
        monkeypatch: Pytest fixture to patch MongoClient.
        caplog: Pytest fixture for capturing logs.
    """
    logger.info("Starting test: connect_to_mongodb_failure")

    def fail_client(*args, **kwargs) -> None:
        # Simulate connection failure
        logger.error("Simulated MongoDB connection failure triggered")
        raise ConnectionFailure("Simulated connection failure")

    # Patch MongoClient to simulate failure
    monkeypatch.setattr("gis_tool.db_utils.MongoClient", fail_client)

    with caplog.at_level(logging.ERROR, logger="gis_tool"):
        with pytest.raises(ConnectionFailure, match="Simulated connection failure"):
            connect_to_mongodb("mongodb://baduri", "test_db")

    # Confirm error log message presence
    assert "Could not connect to MongoDB" in caplog.text

    logger.info("Completed test: connect_to_mongodb_failure")


# ------------------------
# Tests for upsert_mongodb_feature
# ------------------------

def test_upsert_mongodb_feature_insert() -> None:
    """
    Test inserting a new feature document into MongoDB collection.
    Verifies geometry is reprojected to WGS84 and inserted correctly.

    No existing document with the same name and date is found, so insert is expected.
    """
    logger.info("Starting test: upsert_mongodb_feature_insert")

    mock_collection = MagicMock()
    input_geometry = Point(1, 2)

    # Mocked GeoJSON after reprojection (simulated value)
    mocked_geojson = {
        "type": "Point",
        "coordinates": [8.983152841195214e-06, 1.7966305682390134e-05]
    }

    # Patch reproject_to_wgs84 to return mocked GeoJSON
    with patch("gis_tool.db_utils.reproject_to_wgs84", return_value=mocked_geojson):
        # Simulate no existing document found
        mock_collection.find_one.return_value = None

        # Call function to insert new feature
        upsert_mongodb_feature(
            collection=mock_collection,
            name="Line A",
            date="2024-01-01",
            psi=250.0,
            material="Steel",
            geometry=input_geometry,
        )

        # Verify insert_one called once
        mock_collection.insert_one.assert_called_once()
        inserted_doc = mock_collection.insert_one.call_args[0][0]

        # Validate inserted fields
        assert inserted_doc["name"] == "Line A"
        assert inserted_doc["material"] == "Steel"
        assert inserted_doc["geometry"] == mocked_geojson

        # Confirm geometry can be parsed into a Shapely geometry object
        inserted_geom = shape(inserted_doc["geometry"])
        assert isinstance(inserted_geom, BaseGeometry)
        assert inserted_geom.geom_type == "Point"

    logger.info("Completed test: upsert_mongodb_feature_insert")


def test_upsert_mongodb_feature_update() -> None:
    """
    Test updating an existing feature document in MongoDB.

    The existing document is found by name and date; update should modify
    fields if any differ (date, psi, material, geometry).
    """
    logger.info("Starting test: upsert_mongodb_feature_update")

    mock_collection = MagicMock()
    geometry = Point(3, 4)

    # Reproject geometry as would be stored in MongoDB
    reproj_geom = reproject_to_wgs84(geometry)

    # Existing document with reproj geometry
    existing_doc = {
        "_id": "12345",
        "name": "Line B",
        "date": "2023-12-31",
        "psi": 100.0,
        "material": "Iron",
        "geometry": reproj_geom,
    }
    # Mock find_one to return existing document
    mock_collection.find_one.return_value = existing_doc

    # Call function to update feature
    upsert_mongodb_feature(
        collection=mock_collection,
        name="Line B",
        date="2024-01-01",
        psi=120.0,
        material="Steel",
        geometry=geometry,
    )

    # Assert update_one called once with expected filter and update dict
    mock_collection.update_one.assert_called_once()
    filter_arg, update_arg = mock_collection.update_one.call_args[0]

    assert filter_arg == {"_id": "12345"}
    assert "$set" in update_arg
    updated_fields = update_arg["$set"]

    # Confirm fields updated correctly
    assert updated_fields["date"] == "2024-01-01"
    assert updated_fields["psi"] == 120.0
    assert updated_fields["material"] == "Steel"
    # Geometry stays the same as reproj_geom (not changed in update)
    assert existing_doc["geometry"] == reproj_geom

    logger.info("Completed test: upsert_mongodb_feature_update")


def test_upsert_mongodb_feature_no_update() -> None:
    """
    Test that no insert or update occurs if an existing document matches
    all provided values exactly, including geometry.
    """
    logger.info("Starting test: upsert_mongodb_feature_no_update")

    mock_collection = MagicMock()
    geometry = Point(3, 4)

    # Existing document with matching attributes and geometry
    existing_doc = {
        "_id": "99999",
        "name": "Line C",
        "date": "2024-01-01",
        "psi": 250.0,
        "material": "Steel",
        "geometry": mapping(geometry),
    }
    mock_collection.find_one.return_value = existing_doc

    # Call function, expecting no DB insert or update
    upsert_mongodb_feature(
        collection=mock_collection,
        name="Line C",
        date="2024-01-01",
        psi=250.0,
        material="Steel",
        geometry=geometry,
    )

    # Assert no DB operations triggered
    mock_collection.update_one.assert_not_called()
    mock_collection.insert_one.assert_not_called()

    logger.info("Completed test: upsert_mongodb_feature_no_update")
