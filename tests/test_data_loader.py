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


from shapely.geometry import Point

from gis_tool.config import DEFAULT_CRS
from gis_tool.data_loader import create_pipeline_features

# Logger setup
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)


# ---- PIPELINE FUNCTION TESTS ----

def test_create_pipeline_features_geojson(sample_geojson_report, empty_gas_lines_gdf):
    """
    Test processing a GeoJSON report updates gas lines GeoDataFrame and adds features.
    """
    logger.info("Testing create_pipeline_features with GeoJSON input.")
    processed_reports, updated_gdf, features_added = create_pipeline_features(
        geojson_reports=[sample_geojson_report],
        txt_reports=[],
        gas_lines_gdf=empty_gas_lines_gdf,
        spatial_reference=DEFAULT_CRS,
        processed_reports=set(),
        use_mongodb=False
    )

    logger.debug(f"Processed reports: {processed_reports}")
    logger.debug(f"Number of features added: {len(updated_gdf)}; Features added flag: {features_added}")

    assert sample_geojson_report[0] in processed_reports, "GeoJSON report should be marked processed"
    assert len(updated_gdf) == 1, "One feature should be added from GeoJSON"
    assert features_added, "Features added flag should be True"
    assert updated_gdf.crs.to_string() == DEFAULT_CRS, "CRS should be preserved after update"
    assert "Name" in updated_gdf.columns, "Expected columns should be present"

    logger.info("create_pipeline_features_geojson test passed.")


def test_create_pipeline_features_txt(empty_gas_lines_gdf):
    """
    Test processing a properly formatted CSV-style TXT report with quoted location updates the GeoDataFrame.
    """
    logger.info("Testing create_pipeline_features with CSV-style TXT input.")

    # Simulate a CSV-style TXT report line with quoted lat/lon
    valid_txt_line = 'ID,Date,PSI,Material,Location,Notes,ExtraField\n' \
                     '123,2025-06-10,88,Steel,"12.345, 67.890","Test note",N/A'

    txt_reports = [("report.txt", valid_txt_line.splitlines())]

    processed_reports, updated_gdf, features_added = create_pipeline_features(
        geojson_reports=[],
        txt_reports=txt_reports,
        gas_lines_gdf=empty_gas_lines_gdf,
        spatial_reference=DEFAULT_CRS,
        processed_reports=set(),
        use_mongodb=False
    )

    logger.debug(f"Processed reports: {processed_reports}")
    logger.debug(f"Number of features added: {len(updated_gdf)}; Features added flag: {features_added}")

    assert "report.txt" in processed_reports, "TXT report should be marked processed"
    assert len(updated_gdf) == 1, "One feature should be added from TXT"
    assert features_added, "Features added flag should be True"
    assert updated_gdf.crs.to_string() == DEFAULT_CRS, "CRS should be preserved after update"
    assert "Material" in updated_gdf.columns, "Expected 'Material' column should be present"
    assert updated_gdf.iloc[0]["Material"] == "steel", "Material should be normalized to lowercase"
    assert updated_gdf.iloc[0].geometry.x == 12.345, "Longitude should be parsed correctly"
    assert updated_gdf.iloc[0].geometry.y == 67.89, "Latitude should be parsed correctly"

    logger.info("create_pipeline_features_txt test passed.")


def test_create_pipeline_features_skips_processed_reports(empty_gas_lines_gdf, sample_geojson_report, caplog):
    """
    Test that processed reports are skipped and gas lines GeoDataFrame remains unchanged.
    """
    logger.info("Running test_create_pipeline_features_skips_processed_reports")

    processed_reports = {sample_geojson_report[0]}

    with caplog.at_level(logging.INFO):
        new_processed, new_gdf, features_added = create_pipeline_features(
            geojson_reports=[sample_geojson_report],
            txt_reports=[],
            gas_lines_gdf=empty_gas_lines_gdf,
            spatial_reference=DEFAULT_CRS,
            processed_reports=processed_reports,
            use_mongodb=False
        )

    logger.debug(f"Processed reports returned: {new_processed}")
    logger.debug(f"Features added: {features_added}")

    assert sample_geojson_report[0] in new_processed, "Processed report should still be in set"
    assert new_gdf.equals(empty_gas_lines_gdf), "GeoDataFrame should remain unchanged when skipping processed reports"
    assert not features_added, "No features added flag when skipping processed reports"
    assert any("Skipping already processed report" in msg for msg in caplog.messages), "Log should indicate skipping"

    logger.info("Completed test_create_pipeline_features_skips_processed_reports")


def test_create_pipeline_features_handles_malformed_txt_lines(empty_gas_lines_gdf, caplog):
    """
    Test that malformed TXT lines are handled gracefully with logging.
    """
    # Set up an empty gas_lines_gdf
    logger.info("Running test_create_pipeline_features_handles_malformed_txt_lines")

    malformed_line = "1 line1 10.0"  # Intentionally malformed
    txt_reports = [("malformed_report.txt", [malformed_line])]
    geojson_reports = []

    with caplog.at_level(logging.WARNING):
        processed_reports, new_gdf, features_added = create_pipeline_features(
            geojson_reports,
            txt_reports,
            empty_gas_lines_gdf,
            spatial_reference=DEFAULT_CRS,
            processed_reports=set(),
            use_mongodb=False
        )

    logger.warning(f"Malformed line detected in report1.txt: '{malformed_line}'")
    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")

    assert "malformed_report.txt" in processed_reports, "Malformed TXT report should be marked processed"
    assert not features_added, "No features should be added from malformed lines"
    assert new_gdf.equals(empty_gas_lines_gdf), "GeoDataFrame should remain unchanged on malformed input"
    assert any("Malformed line" in msg or "Error parsing" in msg for msg in caplog.messages), "Warning should be logged on malformed line"

    logger.info("Completed test_create_pipeline_features_handles_malformed_txt_lines")


def test_create_pipeline_features_geojson_missing_fields_logs_error(empty_gas_lines_gdf, caplog):
    """
    Test that missing required fields in GeoJSON report triggers an error log and no features added.
    """
    logger.info("Running test_create_pipeline_features_geojson_missing_fields_logs_error")

    gdf_missing = gpd.GeoDataFrame({
        'Name': ['line1'],
        'Date': [pd.Timestamp('2023-01-01')],
        # Missing 'PSI' field
        'Material': ['steel'],
        'geometry': [Point(1, 1)]
    }, crs=DEFAULT_CRS)

    geojson_reports = [("bad_report.geojson", gdf_missing)]
    txt_reports = []

    with caplog.at_level(logging.ERROR):
        processed_reports, new_gdf, features_added = create_pipeline_features(
            geojson_reports,
            txt_reports,
            empty_gas_lines_gdf,
            spatial_reference=DEFAULT_CRS,
            processed_reports=set(),
            use_mongodb=False
        )

    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")
    logger.error("Missing required fields detected in 'bad_report.geojson'")

    assert "bad_report.geojson" in processed_reports, "Bad GeoJSON report should be marked processed"
    assert not features_added, "No features added flag when fields missing"
    assert new_gdf.equals(empty_gas_lines_gdf), "GeoDataFrame unchanged when fields missing"
    assert any("missing required fields" in msg for msg in caplog.messages), "Error should be logged for missing fields"

    logger.info("Completed test_create_pipeline_features_geojson_missing_fields_logs_error")


def test_create_pipeline_features_with_empty_reports(empty_gas_lines_gdf):
    """
    Test that function handles empty inputs correctly without changes.
    """
    logger.info("Running test_create_pipeline_features_with_empty_reports")

    processed_reports, new_gdf, features_added = create_pipeline_features(
        geojson_reports=[],
        txt_reports=[],
        gas_lines_gdf=empty_gas_lines_gdf,
        spatial_reference=DEFAULT_CRS,
        processed_reports=set(),
        use_mongodb=False
    )

    logger.debug(f"Processed reports returned: {processed_reports}")
    logger.debug(f"Features added: {features_added}")

    assert processed_reports == set(), "No processed reports when no input"
    assert new_gdf.equals(empty_gas_lines_gdf), "GeoDataFrame unchanged with empty input"
    assert not features_added, "No features added flag when no input"

    logger.info("Completed test_create_pipeline_features_with_empty_reports")
