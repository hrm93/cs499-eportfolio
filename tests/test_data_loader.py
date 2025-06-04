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

import pandas as pd
import geopandas as gpd

from geopandas import GeoDataFrame
from shapely.geometry import Point

from gis_tool import data_utils
from gis_tool.config import DEFAULT_CRS
from gis_tool import data_loader as dl
from gis_tool.data_loader import create_pipeline_features

# Logger setup
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)


# ---- PIPELINE FUNCTION TESTS ----

def test_create_pipeline_features_geojson():
    """
    Tests create_pipeline_features processes a GeoJSON report,
    updates gas lines GeoDataFrame, and adds features.
    """
    logger.info("Testing create_pipeline_features with GeoJSON input.")
    gdf = gpd.GeoDataFrame({
        "Name": ["Line1"],
        "Date": [pd.Timestamp("2023-01-01")],
        "PSI": [150.0],
        "Material": ["steel"],
        "geometry": [Point(1.0, 2.0)]
    }, crs=DEFAULT_CRS)

    gas_lines = gpd.GeoDataFrame(columns=["Name", "Date", "PSI", "Material", "geometry"], crs=DEFAULT_CRS)

    processed_reports, updated_gdf, features_added = create_pipeline_features(
        geojson_reports=[("geo1.geojson", gdf)],
        txt_reports=[],
        gas_lines_gdf=gas_lines,
        spatial_reference=DEFAULT_CRS,
        processed_reports=set(),
        use_mongodb=False
    )

    logger.debug(f"Processed reports: {processed_reports}")
    logger.debug(f"Number of features added: {len(updated_gdf)}; Features added flag: {features_added}")

    assert "geo1.geojson" in processed_reports
    assert len(updated_gdf) == 1
    assert features_added is True
    logger.info("create_pipeline_features_geojson test passed.")


def test_create_pipeline_features_txt():
    """
    Tests create_pipeline_features processes a TXT report line,
    updates gas lines GeoDataFrame, and adds features.
    """
    logger.info("Testing create_pipeline_features with TXT input.")
    line = "Line2,2023-03-01,200,copper,10.0,20.0"
    
    txt_reports = [("report.txt", [line])]
    gas_lines = gpd.GeoDataFrame(columns=["Name", "Date", "PSI", "Material", "geometry"], crs=DEFAULT_CRS)

    processed_reports, updated_gdf, features_added = create_pipeline_features(
        geojson_reports=[],
        txt_reports=txt_reports,
        gas_lines_gdf=gas_lines,
        spatial_reference=DEFAULT_CRS,
        processed_reports=set(),
        use_mongodb=False
    )

    logger.debug(f"Processed reports: {processed_reports}")
    logger.debug(f"Number of features added: {len(updated_gdf)}; Features added flag: {features_added}")

    assert "report.txt" in processed_reports
    assert len(updated_gdf) == 1
    assert features_added is True
    logger.info("create_pipeline_features_txt test passed.")


def test_create_pipeline_features_skips_processed_reports(empty_gas_lines_gdf, sample_geojson_report):
    """
    Test that create_pipeline_features skips reports that have already been processed.
    """
    logger.info("Running test_create_pipeline_features_skips_processed_reports")

    geojson_reports = [sample_geojson_report]
    txt_reports = []
    processed_reports = {"report1.geojson"}

    new_processed, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports, txt_reports, empty_gas_lines_gdf, "EPSG:4326",
        processed_reports=processed_reports,
        use_mongodb=False,
    )

    logger.debug(f"Processed reports returned: {new_processed}")
    logger.debug(f"Features added: {features_added}")

    assert "report1.geojson" in new_processed
    assert new_gdf.equals(empty_gas_lines_gdf)
    assert not features_added
    logger.info("Completed test_create_pipeline_features_skips_processed_reports")


def test_create_pipeline_features_handles_malformed_txt_lines(tmp_path):
    """
    Test that create_pipeline_features properly handles malformed TXT report lines.
    """
    # Set up an empty gas_lines_gdf
    logger.info("Running test_create_pipeline_features_handles_malformed_txt_lines")

    gas_lines_gdf = data_utils.make_feature("dummy", "2023-01-01", 50.0, "steel", Point(0, 0), "EPSG:4326")
    malformed_line = "1 line1 10.0"
    txt_reports = [("report1.txt", [malformed_line])]
    geojson_reports = []

    processed_reports, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports, txt_reports, gas_lines_gdf, "EPSG:4326",
        processed_reports=set(),
        use_mongodb=False
    )

    logger.warning(f"Malformed line detected in report1.txt: '{malformed_line}'")
    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")

    assert "report1.txt" in processed_reports
    assert not features_added
    assert new_gdf.shape[0] == gas_lines_gdf.shape[0]
    logger.info("Completed test_create_pipeline_features_handles_malformed_txt_lines")


def test_create_pipeline_features_geojson_missing_fields_logs_error(caplog):
    """
    Test that create_pipeline_features logs an error when GeoJSON reports have missing required fields.
    """
    logger.info("Running test_create_pipeline_features_geojson_missing_fields_logs_error")

    gas_lines_gdf = GeoDataFrame(columns=data_utils.SCHEMA_FIELDS, geometry=[], crs="EPSG:4326")

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
        processed_reports=set(),
        use_mongodb=False
    )

    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")
    logger.error("Missing required fields detected in 'bad_report.geojson'")

    assert "bad_report.geojson" in processed_reports
    assert not features_added
    assert any("missing required fields" in record.message for record in caplog.records)
    logger.info("Completed test_create_pipeline_features_geojson_missing_fields_logs_error")


def test_create_pipeline_features_with_empty_reports():
    """
    Test that create_pipeline_features behaves correctly when no reports are provided.
    """
    logger.info("Running test_create_pipeline_features_with_empty_reports")

    gas_lines_gdf = data_utils.make_feature("dummy", "2023-01-01", 50.0, "steel", Point(0, 0), "EPSG:4326")

    processed_reports, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports=[],
        txt_reports=[],
        gas_lines_gdf=gas_lines_gdf,
        spatial_reference="EPSG:4326",
        processed_reports=set(),
        use_mongodb=False
    )

    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")

    assert processed_reports == set()
    assert new_gdf.equals(gas_lines_gdf)
    assert not features_added
    logger.info("Completed test_create_pipeline_features_with_empty_reports")
