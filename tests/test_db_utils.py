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
    Test ensure_spatial_index creates or skips index as appropriate.
    """
    logger.info(f"Starting test_ensure_spatial_index_behavior with index_info={index_info}")

    mock_collection = MagicMock()
    mock_collection.index_information.return_value = index_info

    with caplog.at_level(logging.INFO, logger="gis_tool"):
        ensure_spatial_index(mock_collection)

    if should_create:
        mock_collection.create_index.assert_called_once_with([("geometry", "2dsphere")], name="geometry_2dsphere")
        logger.info("Index creation verified")
    else:
        mock_collection.create_index.assert_not_called()
        logger.info("Index creation skipped as expected")

    assert expected_log in caplog.text
    logger.info("Expected log message found in caplog")


# ------------------------
# Tests for ensure_collection_schema
# ------------------------

def test_ensure_collection_schema_creates_new_collection(caplog: pytest.LogCaptureFixture) -> None:
    """
    Test that ensure_collection_schema creates a new collection with the JSON Schema
    if the collection does not exist.
    """
    logger.info("Starting test: ensure_collection_schema_creates_new_collection")

    mock_db = MagicMock()
    mock_db.create_collection.return_value = None

    with caplog.at_level(logging.INFO, logger="gis_tool"):
        ensure_collection_schema(mock_db, "features", spatial_feature_schema)

    mock_db.create_collection.assert_called_once_with(
        "features",
        validator={"$jsonSchema": spatial_feature_schema},
    )

    assert any(
        phrase in caplog.text for phrase in ["Created new collection", "Ensuring JSON Schema"]
    )

    logger.info("Completed test: ensure_collection_schema_creates_new_collection")


def test_ensure_collection_schema_updates_existing_collection(caplog: pytest.LogCaptureFixture) -> None:
    """
    Test that ensure_collection_schema updates the validator of an existing collection
    using the 'collMod' command when the collection already exists.
    """
    logger.info("Starting test: ensure_collection_schema_updates_existing_collection")

    mock_db = MagicMock()

    def create_collection_side_effect(*args, **kwargs) -> None:
        raise Exception("Collection already exists")

    mock_db.create_collection.side_effect = create_collection_side_effect
    mock_db.command.return_value = {"ok": 1}

    with caplog.at_level(logging.INFO, logger="gis_tool"):
        ensure_collection_schema(mock_db, "features", spatial_feature_schema)

    mock_db.command.assert_called_once_with(
        {
            "collMod": "features",
            "validator": {"$jsonSchema": spatial_feature_schema},
            "validationLevel": "strict",
        }
    )
    assert "Updated schema validator for collection" in caplog.text

    logger.info("Completed test: ensure_collection_schema_updates_existing_collection")


def test_ensure_collection_schema_raises_unexpected_error(caplog: pytest.LogCaptureFixture) -> None:
    """
    Test that ensure_collection_schema raises an exception if an unexpected error occurs
    during collection creation or update.
    """
    logger.info("Starting test: ensure_collection_schema_raises_unexpected_error")

    mock_db = MagicMock()

    def create_collection_side_effect(*args, **kwargs) -> None:
        logger.error("Simulating unexpected error in create_collection")
        raise Exception("Unexpected error")

    mock_db.create_collection.side_effect = create_collection_side_effect

    with caplog.at_level(logging.ERROR, logger="gis_tool"):
        with pytest.raises(Exception, match="Unexpected error"):
            ensure_collection_schema(mock_db, "features", spatial_feature_schema)

    assert "Failed to ensure schema" in caplog.text

    logger.info("Completed test: ensure_collection_schema_raises_unexpected_error")


# ------------------------
# Tests for connect_to_mongodb
# ------------------------

def test_connect_to_mongodb_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test successful connection to MongoDB.
    """
    logger.info("Starting test: connect_to_mongodb_success")

    mock_db = MagicMock()

    class MockMongoClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        @property
        def admin(self) -> MagicMock:
            return MagicMock(command=MagicMock(return_value={"ok": 1}))

        def __getitem__(self, name: str) -> MagicMock:
            return mock_db

    monkeypatch.setattr("gis_tool.db_utils.MongoClient", MockMongoClient)

    result = connect_to_mongodb("mongodb://localhost", "test_db")

    assert result == mock_db

    logger.info("Completed test: connect_to_mongodb_success")


def test_connect_to_mongodb_failure(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """
    Test MongoDB connection failure.
    """
    logger.info("Starting test: connect_to_mongodb_failure")

    def fail_client(*args, **kwargs) -> None:
        logger.error("Simulated MongoDB connection failure triggered")
        raise ConnectionFailure("Simulated connection failure")

    monkeypatch.setattr("gis_tool.db_utils.MongoClient", fail_client)

    with caplog.at_level(logging.ERROR, logger="gis_tool"):
        with pytest.raises(ConnectionFailure, match="Simulated connection failure"):
            connect_to_mongodb("mongodb://baduri", "test_db")

    assert "Could not connect to MongoDB" in caplog.text

    logger.info("Completed test: connect_to_mongodb_failure")


# ------------------------
# Tests for upsert_mongodb_feature
# ------------------------

def test_upsert_mongodb_feature_insert() -> None:
    """
    Test inserting a new feature into MongoDB collection.
    Ensures geometry is correctly reprojected and stored.
    """
    logger.info("Starting test: upsert_mongodb_feature_insert")

    mock_collection = MagicMock()
    input_geometry = Point(1, 2)

    # Define known reprojected GeoJSON result (mocked)
    mocked_geojson = {
        "type": "Point",
        "coordinates": [8.983152841195214e-06, 1.7966305682390134e-05]
    }

    with patch("gis_tool.db_utils.reproject_to_wgs84", return_value=mocked_geojson):
        mock_collection.find_one.return_value = None

        upsert_mongodb_feature(
            collection=mock_collection,
            name="Line A",
            date="2024-01-01",
            psi=250.0,
            material="Steel",
            geometry=input_geometry,
        )

        mock_collection.insert_one.assert_called_once()
        inserted_doc = mock_collection.insert_one.call_args[0][0]

        assert inserted_doc["name"] == "Line A"
        assert inserted_doc["material"] == "Steel"
        assert inserted_doc["geometry"] == mocked_geojson

        # Confirm shape parses correctly
        inserted_geom = shape(inserted_doc["geometry"])
        assert isinstance(inserted_geom, BaseGeometry)
        assert inserted_geom.geom_type == "Point"
    logger.info("Completed test: upsert_mongodb_feature_insert")


def test_upsert_mongodb_feature_update() -> None:
    """
    Test updating an existing MongoDB feature with GeoJSON geometry.
    """
    logger.info("Starting test: upsert_mongodb_feature_update")

    mock_collection = MagicMock()
    geometry = Point(3, 4)

    # Use reproject_to_wgs84 to mimic what MongoDB would store
    reproj_geom = reproject_to_wgs84(geometry)

    # Existing document in MongoDB with reprojected geometry
    existing_doc = {
        "_id": "12345",
        "name": "Line B",
        "date": "2023-12-31",
        "psi": 100.0,
        "material": "Iron",
        "geometry": reproj_geom,
    }
    mock_collection.find_one.return_value = existing_doc

    upsert_mongodb_feature(
        collection=mock_collection,
        name="Line B",
        date="2024-01-01",
        psi=120.0,
        material="Steel",
        geometry=geometry,
    )

    # update_one should be called with correct filter and update arguments
    mock_collection.update_one.assert_called_once()
    filter_arg, update_arg = mock_collection.update_one.call_args[0]

    assert filter_arg == {"_id": "12345"}
    assert "$set" in update_arg
    updated_fields = update_arg["$set"]

    assert updated_fields["date"] == "2024-01-01"
    assert updated_fields["psi"] == 120.0
    assert updated_fields["material"] == "Steel"
    assert existing_doc["geometry"] == reproj_geom

    logger.info("Completed test: upsert_mongodb_feature_update")


def test_upsert_mongodb_feature_no_update() -> None:
    """
    Test no update or insert if existing document matches all provided values.
    """
    logger.info("Starting test: upsert_mongodb_feature_no_update")

    mock_collection = MagicMock()
    geometry = Point(3, 4)

    existing_doc = {
        "_id": "99999",
        "name": "Line C",
        "date": "2024-01-01",
        "psi": 250.0,
        "material": "Steel",
        "geometry": mapping(geometry),
    }
    mock_collection.find_one.return_value = existing_doc

    upsert_mongodb_feature(
        collection=mock_collection,
        name="Line C",
        date="2024-01-01",
        psi=250.0,
        material="Steel",
        geometry=geometry,
    )

    mock_collection.update_one.assert_not_called()
    mock_collection.insert_one.assert_not_called()

    logger.info("Completed test: upsert_mongodb_feature_no_update")
