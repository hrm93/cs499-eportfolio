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
import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import geopandas as gpd
import pytest
from geopandas import GeoDataFrame
from shapely.geometry import Point

from gis_tool import data_loader as dl
from gis_tool.data_loader import (
    make_feature,
    create_pipeline_features,
)
from gis_tool.report_reader import (
    find_new_reports,
    load_geojson_report,
    load_txt_report_lines,
)

# Constants
CRS = "EPSG:4326"

# Logger setup
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)


# ---- FIXTURES ----

@pytest.fixture
def sample_geojson_report():
    """
     Provides a sample GeoJSON report as a tuple of filename and GeoDataFrame.
     The GeoDataFrame contains a single feature with predefined attributes and geometry.
     """
    point = Point(1, 1)
    gdf = GeoDataFrame({
        'Name': ['line1'],
        'Date': [pd.Timestamp('2023-01-01')],
        'PSI': [50.0],
        'Material': ['steel'],
        'geometry': [point]
    }, crs="EPSG:4326")
    logger.debug("Created sample_geojson_report fixture with one feature.")
    return "report1.geojson", gdf


@pytest.fixture
def empty_gas_lines_gdf():
    """
    Provides an empty GeoDataFrame with the schema defined in dl.SCHEMA_FIELDS,
    suitable for testing functions that expect an empty gas lines dataset.
    """
    logger.debug("Created empty_gas_lines_gdf fixture.")
    return GeoDataFrame(columns=dl.SCHEMA_FIELDS, geometry=[], crs="EPSG:4326")


# ---- UNIT TESTS ----

def test_make_feature_creates_valid_gdf():
    """
    Tests make_feature creates a GeoDataFrame with the expected schema,
    applies case normalization to material, and sets the CRS.
    """
    logger.info("Testing make_feature function.")
    feature = make_feature("LineX", "2022-05-01", 300, "PVC", Point(1, 2), CRS)
    assert isinstance(feature, gpd.GeoDataFrame)
    assert feature.iloc[0]["Material"] == "pvc"
    assert feature.crs.to_string() == CRS
    logger.debug("make_feature created a valid GeoDataFrame with correct CRS and normalized material.")
    logger.info("make_feature test passed.")


# ---- FILE I/O TESTS ----

def test_find_new_reports(tmp_path):
    """
    Tests that find_new_reports correctly identifies .txt and .geojson files in a directory,
    ignoring other file types.
    """
    logger.info("Testing find_new_reports function.")
    txt_file = tmp_path / "test.txt"
    geojson_file = tmp_path / "test.geojson"
    other_file = tmp_path / "ignore.csv"

    txt_file.write_text("Test")
    geojson_file.write_text("{}")
    other_file.write_text("Should not be picked up")

    result = find_new_reports(str(tmp_path))
    logger.debug(f"find_new_reports found files: {result}")
    assert sorted(result) == sorted(["test.txt", "test.geojson"])
    logger.info("find_new_reports test passed.")


def test_load_txt_report_lines(tmp_path):
    """
    Tests load_txt_report_lines reads lines from a text report file,
    verifying it reads the header and data lines correctly.
    """
    logger.info("Testing load_txt_report_lines function.")
    file_path = tmp_path / "report.txt"
    content = "Id Name X Y Date PSI Material\n1 Line1 10.0 20.0 2023-01-01 250 steel"
    file_path.write_text(content)

    lines = load_txt_report_lines(str(file_path))
    logger.debug(f"Loaded {len(lines)} lines from TXT report.")
    assert len(lines) == 2                       # Header + one data row
    assert "Line1" in lines[1]
    logger.info("load_txt_report_lines test passed.")


def test_load_geojson_report_crs(tmp_path):
    """
    Tests load_geojson_report loads a GeoJSON file and reprojects
    the GeoDataFrame to the expected CRS.
    """
    logger.info("Testing load_geojson_report function.")
    gdf = gpd.GeoDataFrame({
        "Name": ["LineA"],
        "Date": [pd.Timestamp("2024-01-01")],
        "PSI": [100.0],
        "Material": ["steel"],
        "geometry": [Point(1.0, 1.0)]
    }, crs="EPSG:3857")

    file_path = tmp_path / "test.geojson"
    gdf.to_file(str(file_path), driver="GeoJSON")

    result_gdf = load_geojson_report(file_path, "EPSG:4326")
    logger.debug(f"Loaded GeoJSON report with CRS: {result_gdf.crs.to_string()}")
    assert result_gdf.crs.to_string() == "EPSG:4326"
    assert "Name" in result_gdf.columns
    logger.info("load_geojson_report test passed.")


# ---- PIPELINE FUNCTION TESTS ----

@patch("gis_tool.data_loader.upsert_mongodb_feature")
def test_create_pipeline_features_geojson(mock_upsert):
    """
    Tests create_pipeline_features processes a GeoJSON report,
    updates gas lines GeoDataFrame, adds features, and calls MongoDB upsert correctly.
    """
    logger.info("Testing create_pipeline_features with GeoJSON input.")
    gdf = gpd.GeoDataFrame({
        "Name": ["Line1"],
        "Date": [pd.Timestamp("2023-01-01")],
        "PSI": [150.0],
        "Material": ["steel"],
        "geometry": [Point(1.0, 2.0)]
    }, crs=CRS)

    gas_lines = gpd.GeoDataFrame(columns=["Name", "Date", "PSI", "Material", "geometry"], crs=CRS)
    mock_collection = MagicMock()

    processed_reports, updated_gdf, features_added = create_pipeline_features(
        geojson_reports=[("geo1.geojson", gdf)],
        txt_reports=[],
        gas_lines_gdf=gas_lines,
        spatial_reference=CRS,
        gas_lines_collection=mock_collection,
        processed_reports=set(),
        use_mongodb=True
    )

    logger.debug(f"Processed reports: {processed_reports}")
    logger.debug(f"Number of features added: {len(updated_gdf)}; Features added flag: {features_added}")

    assert "geo1.geojson" in processed_reports
    assert len(updated_gdf) == 1
    assert features_added is True

    called_args = mock_upsert.call_args[0]
    assert called_args[0] == mock_collection
    assert called_args[1] == "Line1"
    assert pd.Timestamp(called_args[2]) == pd.Timestamp("2023-01-01")
    assert called_args[3] == 150.0
    assert called_args[4] == "steel"
    assert called_args[5] == Point(1.0, 2.0)

    mock_upsert.assert_called_once()
    logger.info("create_pipeline_features_geojson test passed.")


@patch("gis_tool.data_loader.upsert_mongodb_feature")
def test_create_pipeline_features_txt(mock_upsert):
    """
    Tests create_pipeline_features processes a TXT report line,
    updates gas lines GeoDataFrame, adds features, and calls MongoDB upsert correctly.
    """
    logger.info("Testing create_pipeline_features with TXT input.")
    line = "1 Line2 10.0 20.0 2023-03-01 200 copper"
    txt_reports = [("report.txt", [line])]
    gas_lines = gpd.GeoDataFrame(columns=["Name", "Date", "PSI", "Material", "geometry"], crs=CRS)
    mock_collection = MagicMock()

    processed_reports, updated_gdf, features_added = create_pipeline_features(
        geojson_reports=[],
        txt_reports=txt_reports,
        gas_lines_gdf=gas_lines,
        spatial_reference=CRS,
        gas_lines_collection=mock_collection,
        processed_reports=set(),
        use_mongodb=True
    )

    logger.debug(f"Processed reports: {processed_reports}")
    logger.debug(f"Number of features added: {len(updated_gdf)}; Features added flag: {features_added}")

    assert "report.txt" in processed_reports
    assert len(updated_gdf) == 1
    assert features_added is True

    called_args = mock_upsert.call_args[0]
    assert called_args[0] == mock_collection
    assert called_args[1] == "Line2"
    assert pd.Timestamp(called_args[2]) == pd.Timestamp("2023-03-01")
    assert called_args[3] == 200.0
    assert called_args[4] == "copper"
    assert called_args[5] == Point(10.0, 20.0)

    mock_upsert.assert_called_once()
    logger.info("create_pipeline_features_txt test passed.")


def test_create_pipeline_features_skips_processed_reports(empty_gas_lines_gdf, sample_geojson_report):
    """
       Test that create_pipeline_features skips reports that have already been processed.

       Given:
           - A list of GeoJSON reports including one that is already marked as processed.
           - An empty GeoDataFrame for existing gas lines.
           - A mock MongoDB collection.

       When:
           - create_pipeline_features is called with these inputs.

       Then:
           - The processed report is included in the returned processed_reports set.
           - The gas lines GeoDataFrame remains unchanged.
           - No new features are added.
           - The MongoDB collection is not called (no database operations).
       """
    logger.info("Running test_create_pipeline_features_skips_processed_reports")

    geojson_reports = [sample_geojson_report]
    txt_reports = []
    processed_reports = {"report1.geojson"}
    mock_collection = MagicMock()

    new_processed, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports, txt_reports, empty_gas_lines_gdf, "EPSG:4326",
        gas_lines_collection=mock_collection,
        processed_reports=processed_reports,
        use_mongodb=True,
    )

    logger.debug(f"Processed reports returned: {new_processed}")
    logger.debug(f"Features added: {features_added}")

    assert "report1.geojson" in new_processed
    assert new_gdf.equals(empty_gas_lines_gdf)
    assert not features_added
    mock_collection.assert_not_called()
    logger.info("Completed test_create_pipeline_features_skips_processed_reports")


def test_create_pipeline_features_handles_malformed_txt_lines(tmp_path):
    """
      Test that create_pipeline_features properly handles malformed TXT report lines.

      Given:
          - A gas lines GeoDataFrame initialized with one dummy feature.
          - TXT report lines that are malformed (fewer than required fields).
          - No GeoJSON reports.
          - A mock MongoDB collection.

      When:
          - create_pipeline_features processes the malformed TXT lines.

      Then:
          - The malformed report is marked as processed.
          - No new features are added.
          - The MongoDB collection is not called.
          - The returned gas lines GeoDataFrame remains unchanged in number of rows.
      """
    # Set up an empty gas_lines_gdf
    logger.info("Running test_create_pipeline_features_handles_malformed_txt_lines")

    gas_lines_gdf = dl.make_feature("dummy", "2023-01-01", 50.0, "steel", Point(0, 0), "EPSG:4326")
    malformed_line = "1 line1 10.0"
    txt_reports = [("report1.txt", [malformed_line])]
    geojson_reports = []
    mock_collection = MagicMock()

    processed_reports, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports, txt_reports, gas_lines_gdf, "EPSG:4326",
        gas_lines_collection=mock_collection,
        processed_reports=set(),
        use_mongodb=True
    )

    logger.warning(f"Malformed line detected in report1.txt: '{malformed_line}'")
    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")

    assert "report1.txt" in processed_reports
    assert not features_added
    mock_collection.assert_not_called()
    assert new_gdf.shape[0] == gas_lines_gdf.shape[0]
    logger.info("Completed test_create_pipeline_features_handles_malformed_txt_lines")


def test_create_pipeline_features_geojson_missing_fields_logs_error(caplog):
    """
      Test that create_pipeline_features logs an error when GeoJSON reports have missing required fields.

      Given:
          - An empty gas lines GeoDataFrame.
          - A GeoJSON report missing the 'PSI' field.
          - No TXT reports.
          - No MongoDB collection.

      When:
          - create_pipeline_features is called with the incomplete GeoJSON report.

      Then:
          - The report is marked as processed despite being skipped.
          - No features are added.
          - An error message indicating missing required fields is logged.
      """
    logger.info("Running test_create_pipeline_features_geojson_missing_fields_logs_error")

    gas_lines_gdf = GeoDataFrame(columns=dl.SCHEMA_FIELDS, geometry=[], crs="EPSG:4326")

    gdf_missing = GeoDataFrame({
        'Name': ['line1'],
        'Date': [pd.Timestamp('2023-01-01')],
        'Material': ['steel'],
        'geometry': [Point(1, 1)]
    }, crs="EPSG:4326")

    geojson_reports = [("bad_report.geojson", gdf_missing)]
    txt_reports = []

    processed_reports, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports, txt_reports, gas_lines_gdf, "EPSG:4326",
        gas_lines_collection=None,
        processed_reports=set(),
        use_mongodb=True
    )

    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")
    logger.error("Missing required fields detected in 'bad_report.geojson'")

    assert "bad_report.geojson" in processed_reports
    assert not features_added
    assert any("missing required fields" in record.message for record in caplog.records)
    logger.info("Completed test_create_pipeline_features_geojson_missing_fields_logs_error")


def test_create_pipeline_features_with_use_mongodb_false_does_not_call_mongo(empty_gas_lines_gdf, sample_geojson_report):
    """
     Test that create_pipeline_features does not call MongoDB operations when use_mongodb is False.

     Given:
         - A list containing one GeoJSON report.
         - An empty gas lines GeoDataFrame.
         - A mock MongoDB collection.
         - use_mongodb flag set to False.

     When:
         - create_pipeline_features is called.

     Then:
         - The report is marked as processed.
         - New features are added.
         - The MongoDB collection is not called.
     """
    logger.info("Running test_create_pipeline_features_with_use_mongodb_false_does_not_call_mongo")

    geojson_reports = [sample_geojson_report]
    txt_reports = []
    mock_collection = MagicMock()

    processed_reports, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports, txt_reports, empty_gas_lines_gdf, "EPSG:4326",
        gas_lines_collection=mock_collection,
        processed_reports=set(),
        use_mongodb=False,
    )

    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")
    logger.info("MongoDB calls skipped due to use_mongodb=False")

    assert "report1.geojson" in processed_reports
    assert features_added
    mock_collection.assert_not_called()
    logger.info("Completed test_create_pipeline_features_with_use_mongodb_false_does_not_call_mongo")


def test_create_pipeline_features_with_empty_reports():
    """
      Test that create_pipeline_features behaves correctly when no reports are provided.

      Given:
          - An existing gas lines GeoDataFrame with one dummy feature.
          - No GeoJSON reports.
          - No TXT reports.
          - No MongoDB collection.

      When:
          - create_pipeline_features is called.

      Then:
          - No reports are marked as processed.
          - The gas lines GeoDataFrame remains unchanged.
          - No new features are added.
      """
    logger.info("Running test_create_pipeline_features_with_empty_reports")

    gas_lines_gdf = dl.make_feature("dummy", "2023-01-01", 50.0, "steel", Point(0, 0), "EPSG:4326")

    processed_reports, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports=[],
        txt_reports=[],
        gas_lines_gdf=gas_lines_gdf,
        spatial_reference="EPSG:4326",
        gas_lines_collection=None,
        processed_reports=set(),
        use_mongodb=True
    )

    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")

    assert processed_reports == set()
    assert new_gdf.equals(gas_lines_gdf)
    assert not features_added
    logger.info("Completed test_create_pipeline_features_with_empty_reports")
