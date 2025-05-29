import pytest
from unittest.mock import MagicMock
from pymongo.errors import ConnectionFailure
from shapely.geometry import Point

from gis_tool.db_utils import connect_to_mongodb, upsert_mongodb_feature


# ------------------------
# Tests for connect_to_mongodb
# ------------------------

def test_connect_to_mongodb_success(monkeypatch):
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
    assert result == mock_db


def test_connect_to_mongodb_failure(monkeypatch, caplog):
    def fail_client(*args, **kwargs):
        raise ConnectionFailure("Simulated connection failure")

    monkeypatch.setattr("gis_tool.db_utils.MongoClient", fail_client)

    with caplog.at_level("ERROR", logger="gis_tool"):
        with pytest.raises(ConnectionFailure, match="Simulated connection failure"):
            connect_to_mongodb("mongodb://baduri", "test_db")

    assert "Could not connect to MongoDB" in caplog.text


# ------------------------
# Tests for upsert_mongodb_feature
# ------------------------

def test_upsert_mongodb_feature_insert(monkeypatch):
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
    assert inserted_doc["name"] == "Line A"
    assert inserted_doc["material"] == "Steel"
    assert "geometry" in inserted_doc


def test_upsert_mongodb_feature_update(monkeypatch):
    mock_collection = MagicMock()
    geometry = Point(3, 4)

    existing_doc = {
        "_id": "12345",
        "name": "Line B",
        "date": "2023-12-31",
        "psi": 100.0,
        "material": "Iron",
        "geometry": geometry.wkt  # Simplified geometry is .wkt
    }

    monkeypatch.setattr(
        "gis_tool.db_utils.simplify_geometry",
        lambda g: g.wkt  # match existing_doc
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
    assert update_call[0] == {"_id": "12345"}
    assert "$set" in mock_collection.update_one.call_args[0][1]


def test_upsert_mongodb_feature_no_update(monkeypatch):
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
