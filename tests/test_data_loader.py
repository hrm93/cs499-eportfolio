### test_data_loader
"""
test_data_loader.py

Unit tests for the gis_tool.data_loader module.

Tests cover:
- MongoDB connection handling with success and failure scenarios.
- Detection of new report files in directories.
- Parsing and feature creation from both text (.txt) and GeoJSON (.geojson) report files.
- Skipping already processed reports to avoid duplicates.
- Integration with MongoDB collection mock to verify insert/update operations.
- Validation of shapefile creation and feature persistence.

Uses pytest fixtures to create isolated, temporary test environments and
mock objects to isolate dependencies, enabling reliable and repeatable tests.

Test framework: pytest
"""

import os
import tempfile
import geopandas as gpd
import pytest
from unittest.mock import MagicMock
from gis_tool.data_loader import find_new_reports, create_pipeline_features, connect_to_mongodb
from unittest import mock
from shapely.geometry import Point
from pathlib import Path


def test_connect_to_mongodb_success(monkeypatch):
    """Test successful MongoDB connection using a mocked client."""
    mock_client = MagicMock()
    mock_db = MagicMock()

    # Patch MongoClient in the data_loader module
    monkeypatch.setattr("gis_tool.data_loader.MongoClient", lambda *args, **kwargs: mock_client)
    mock_client.admin.command.return_value = {"ok": 1}
    mock_client.__getitem__.return_value = mock_db

    db = connect_to_mongodb("mongodb://fakeuri", "test_db")
    assert db == mock_db, "MongoDB connection did not return expected database instance."


def test_find_new_reports_creates_list():
    """Test that find_new_reports returns only .txt and .geojson files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        (tmp_path / "file1.txt").touch()
        (tmp_path / "file2.doc").touch()
        reports = find_new_reports(str(tmp_path))
        assert "file1.txt" in reports, "Expected .txt file missing from reports list."
        assert "file2.doc" not in reports, "Non-.txt file incorrectly included in reports list."


def test_find_new_reports_no_txt_files():
    """Test find_new_reports returns empty list if no .txt or .geojson files found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        (tmp_path / "file1.doc").touch()
        reports = find_new_reports(str(tmp_path))
        assert reports == [], "Expected empty list when no .txt or .geojson files present."


@pytest.fixture
def setup_reports_folder():
    """Fixture: Creates a temp folder with one valid and one malformed .txt report file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        valid_report = tmp_path / "report1.txt"
        valid_report.write_text("Id Name X Y Date PSI Material ExtraField\n1 LineA 100 200 2024-05-10 150.0 steel extra\n")

        malformed_report = tmp_path / "report2.txt"
        malformed_report.write_text("Malformed line without enough fields\n")

        yield str(tmp_path), [str(valid_report), str(malformed_report)]


@pytest.fixture
def setup_reports_folder_with_geojson():
    """Fixture: Creates a temp folder with a valid GeoJSON report."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        geojson_path = tmp_path / "report3.geojson"
        gdf = gpd.GeoDataFrame({
            "Name": ["LineGeo"],
            "Date": ["2025-05-15"],
            "PSI": [120.0],
            "Material": ["plastic"],
            "geometry": [Point(50.0, 60.0)]
        }, crs="EPSG:32633")
        gdf.to_file(str(geojson_path), driver="GeoJSON")
        yield str(tmp_path), [str(geojson_path)]


def test_create_pipeline_features_basic(setup_reports_folder):
    """Test create_pipeline_features creates shapefile with new features from valid reports."""
    reports_folder, report_files = setup_reports_folder
    gas_lines_shp = Path(reports_folder) / "gas_lines.shp"
    spatial_ref = "EPSG:32633"

    create_pipeline_features(report_files, str(gas_lines_shp), reports_folder, spatial_ref, gas_lines_collection=None)

    gdf = gpd.read_file(str(gas_lines_shp))
    assert not gdf.empty, "Shapefile GeoDataFrame should not be empty after adding features."
    assert "LineA" in gdf["Name"].values, "Expected feature 'LineA' missing from shapefile."


def test_create_pipeline_features_with_geojson(setup_reports_folder_with_geojson):
    """Test create_pipeline_features can parse and add features from .geojson reports."""
    reports_folder, report_files = setup_reports_folder_with_geojson
    gas_lines_shp = Path(reports_folder) / "gas_lines.shp"
    spatial_ref = "EPSG:32633"

    create_pipeline_features(report_files, str(gas_lines_shp), reports_folder, spatial_ref, gas_lines_collection=None)

    gdf = gpd.read_file(str(gas_lines_shp))
    assert not gdf.empty, "Shapefile GeoDataFrame should not be empty after adding GeoJSON features."
    assert "LineGeo" in gdf["Name"].values, "Expected feature 'LineGeo' missing from shapefile."


def test_create_pipeline_features_skips_processed(setup_reports_folder):
    """Test that already processed reports are skipped, resulting in no duplicate features."""
    reports_folder, report_files = setup_reports_folder
    gas_lines_shp = Path(reports_folder) / "gas_lines.shp"
    spatial_ref = "EPSG:32633"
    processed = {"report1.txt"}

    # Create empty shapefile first (simulate existing)
    empty_gdf = gpd.GeoDataFrame(columns=["Name", "Date", "PSI", "Material", "geometry"], crs=spatial_ref)
    empty_gdf.to_file(str(gas_lines_shp))

    create_pipeline_features(report_files, str(gas_lines_shp), reports_folder, spatial_ref, gas_lines_collection=None, processed_reports=processed)

    gdf = gpd.read_file(str(gas_lines_shp))
    assert gdf.empty, "No features should be added when reports are marked as processed."


def test_create_pipeline_features_with_mongodb_inserts(setup_reports_folder):
    """Test create_pipeline_features inserts new features into MongoDB collection mock."""
    reports_folder, report_files = setup_reports_folder
    gas_lines_shp = Path(reports_folder) / "gas_lines.shp"
    spatial_ref = "EPSG:32633"

    mock_collection = MagicMock()
    mock_collection.find_one.return_value = None  # Simulate new feature not found

    create_pipeline_features(report_files, str(gas_lines_shp), reports_folder, spatial_ref, gas_lines_collection=mock_collection)

    assert mock_collection.insert_one.called, "MongoDB insert_one was not called for new features."
    inserted_feature = mock_collection.insert_one.call_args[0][0]
    assert inserted_feature['name'] == "LineA", "Inserted MongoDB document has incorrect 'name' field."
    assert inserted_feature['material'] == "steel", "Inserted MongoDB document has incorrect 'material' field."


def test_create_pipeline_features_with_mongodb_update(setup_reports_folder):
    """Test that existing MongoDB features are updated instead of inserted."""
    reports_folder, report_files = setup_reports_folder
    gas_lines_shp = Path(reports_folder) / "gas_lines.shp"
    spatial_ref = "EPSG:32633"

    mock_collection = MagicMock()
    # Simulate existing feature found
    mock_collection.find_one.return_value = {"_id": "existing_id"}

    create_pipeline_features(report_files, str(gas_lines_shp), reports_folder, spatial_ref, gas_lines_collection=mock_collection)

    assert mock_collection.update_one.called, "MongoDB update_one was not called for existing features."
    # insert_one should not be called because feature exists
    assert not mock_collection.insert_one.called, "MongoDB insert_one should not be called for existing features."
