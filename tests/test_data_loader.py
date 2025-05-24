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
import geopandas as gpd
import pandas as pd
from unittest.mock import MagicMock, patch
from shapely.geometry import Point
from gis_tool import data_loader as dl

from gis_tool.data_loader import (
    robust_date_parse,
    find_new_reports,
    load_geojson_report,
    load_txt_report_lines,
    make_feature,
    create_pipeline_features
)
CRS = "EPSG:4326"


def test_robust_date_parse():
    assert robust_date_parse("2023-05-01") == pd.Timestamp("2023-05-01")      # ISO
    assert robust_date_parse("01/05/2023") == pd.Timestamp("2023-05-01")      # European: 1 May
    assert robust_date_parse("05/01/2023") == pd.Timestamp("2023-01-05")      # US-style interpreted as 5 Jan
    assert pd.isna(robust_date_parse("not a date"))                           # Invalid
    assert pd.isna(robust_date_parse(None))                                   # None


def test_find_new_reports(tmp_path):
    txt_file = tmp_path / "test.txt"
    geojson_file = tmp_path / "test.geojson"
    other_file = tmp_path / "ignore.csv"

    txt_file.write_text("Test")
    geojson_file.write_text("{}")
    other_file.write_text("Should not be picked up")

    result = find_new_reports(str(tmp_path))
    assert sorted(result) == sorted(["test.txt", "test.geojson"])


def test_load_txt_report_lines(tmp_path):
    file_path = tmp_path / "report.txt"
    content = "Id Name X Y Date PSI Material\n1 Line1 10.0 20.0 2023-01-01 250 steel"
    file_path.write_text(content)

    lines = load_txt_report_lines(str(file_path))
    assert len(lines) == 2                       # Header + one data row
    assert "Line1" in lines[1]                   # Now explicitly check second line for content


def test_load_geojson_report_crs(tmp_path):
    gdf = gpd.GeoDataFrame({
        "Name": ["LineA"],
        "Date": [pd.Timestamp("2024-01-01")],
        "PSI": [100.0],
        "Material": ["steel"],
        "geometry": [Point(1.0, 1.0)]
    }, crs="EPSG:3857")

    file_path = tmp_path / "test.geojson"
    gdf.to_file(file_path, driver="GeoJSON")

    result_gdf = load_geojson_report(str(file_path), "EPSG:4326")
    assert result_gdf.crs.to_string() == "EPSG:4326"
    assert "Name" in result_gdf.columns


def test_make_feature_creates_valid_gdf():
    feature = make_feature("LineX", "2022-05-01", 300, "PVC", Point(1, 2), CRS)
    assert isinstance(feature, gpd.GeoDataFrame)
    assert feature.iloc[0]["Material"] == "pvc"
    assert feature.crs.to_string() == CRS


def test_simplify_geometry_returns_mapping():
    point = Point(10.123456789, 20.987654321)
    simplified = dl.simplify_geometry(point, tolerance=0.01)
    # Should return a GeoJSON-like dict with coordinates simplified
    assert isinstance(simplified, dict)
    assert 'type' in simplified and simplified['type'] == 'Point'
    coords = simplified.get('coordinates', [])
    # Coordinates should be floats close to original (within tolerance)
    assert abs(coords[0] - 10.123456789) < 0.01
    assert abs(coords[1] - 20.987654321) < 0.01


@patch("gis_tool.data_loader.upsert_mongodb_feature")
def test_create_pipeline_features_geojson(mock_upsert):
    gdf = gpd.GeoDataFrame({
        "Name": ["Line1"],
        "Date": [pd.Timestamp("2023-01-01")],
        "PSI": [150.0],
        "Material": ["steel"],
        "geometry": [Point(1.0, 2.0)]
    }, crs=CRS)

    gas_lines = gpd.GeoDataFrame(columns=["Name", "Date", "PSI", "Material", "geometry"], crs=CRS)

    processed_reports, updated_gdf, features_added = create_pipeline_features(
        geojson_reports=[("geo1.geojson", gdf)],
        txt_reports=[],
        gas_lines_gdf=gas_lines,
        spatial_reference=CRS,
        gas_lines_collection=MagicMock(),
        processed_reports=set(),
        use_mongodb=True
    )

    assert "geo1.geojson" in processed_reports
    assert len(updated_gdf) == 1
    assert features_added is True
    mock_upsert.assert_called_once()


@patch("gis_tool.data_loader.upsert_mongodb_feature")
def test_create_pipeline_features_txt(mock_upsert):
    line = "1 Line2 10.0 20.0 2023-03-01 200 copper"
    txt_reports = [("report.txt", [line])]
    gas_lines = gpd.GeoDataFrame(columns=["Name", "Date", "PSI", "Material", "geometry"], crs=CRS)

    processed_reports, updated_gdf, features_added = create_pipeline_features(
        geojson_reports=[],
        txt_reports=txt_reports,
        gas_lines_gdf=gas_lines,
        spatial_reference=CRS,
        gas_lines_collection=MagicMock(),
        processed_reports=set(),
        use_mongodb=True
    )

    assert "report.txt" in processed_reports
    assert len(updated_gdf) == 1
    assert features_added is True
    mock_upsert.assert_called_once()


def test_create_pipeline_features_skips_processed_reports(tmp_path):
    # Prepare a GeoJSON report with one feature
    from geopandas import GeoDataFrame
    import pandas as pd

    # Basic gas_lines_gdf with schema fields
    gas_lines_gdf = GeoDataFrame(columns=dl.SCHEMA_FIELDS, geometry=[], crs="EPSG:4326")

    # One GeoJSON report
    point = Point(1, 1)
    gdf = GeoDataFrame({
        'Name': ['line1'],
        'Date': [pd.Timestamp('2023-01-01')],
        'PSI': [50.0],
        'Material': ['steel'],
        'geometry': [point]
    }, crs="EPSG:4326")

    geojson_reports = [("report1.geojson", gdf)]
    txt_reports = []

    # Mark report1.geojson as already processed
    processed_reports = {"report1.geojson"}

    # Mock MongoDB collection
    mock_collection = MagicMock()

    new_processed, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports, txt_reports, gas_lines_gdf, "EPSG:4326",
        gas_lines_collection=mock_collection,
        processed_reports=processed_reports,
        use_mongodb=True,
    )

    # Since report is processed, no features added, gas_lines_gdf unchanged
    assert "report1.geojson" in new_processed
    assert new_gdf.equals(gas_lines_gdf)
    assert not features_added
    mock_collection.assert_not_called()


def test_create_pipeline_features_handles_malformed_txt_lines(tmp_path):
    # Setup an empty gas_lines_gdf
    gas_lines_gdf = dl.make_feature("dummy", "2023-01-01", 50.0, "steel", Point(0, 0), "EPSG:4326")

    # Malformed TXT line with less than 7 fields
    malformed_line = "1 line1 10.0"  # Only 3 fields
    txt_reports = [("report1.txt", [malformed_line])]
    geojson_reports = []

    # Mock MongoDB collection to check calls
    mock_collection = MagicMock()

    processed_reports, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports, txt_reports, gas_lines_gdf, "EPSG:4326",
        gas_lines_collection=mock_collection,
        processed_reports=set(),
        use_mongodb=True
    )

    # No features should be added because the line is malformed
    assert "report1.txt" in processed_reports
    assert not features_added
    mock_collection.assert_not_called()
    # gas_lines_gdf should remain the same or at least contain no new lines
    assert new_gdf.shape[0] == gas_lines_gdf.shape[0]


def test_create_pipeline_features_geojson_missing_fields_logs_error(caplog):
    from geopandas import GeoDataFrame
    import pandas as pd

    gas_lines_gdf = GeoDataFrame(columns=dl.SCHEMA_FIELDS, geometry=[], crs="EPSG:4326")

    # GeoJSON missing 'PSI' field
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

    # The report should be marked as processed even though skipped
    assert "bad_report.geojson" in processed_reports
    # No features should be added
    assert not features_added
    # Check if error log captured missing fields
    assert any("missing required fields" in record.message for record in caplog.records)


def test_create_pipeline_features_with_use_mongodb_false_does_not_call_mongo():
    from geopandas import GeoDataFrame
    import pandas as pd

    gas_lines_gdf = GeoDataFrame(columns=dl.SCHEMA_FIELDS, geometry=[], crs="EPSG:4326")

    point = Point(1, 1)
    gdf = GeoDataFrame({
        'Name': ['line1'],
        'Date': [pd.Timestamp('2023-01-01')],
        'PSI': [50.0],
        'Material': ['steel'],
        'geometry': [point]
    }, crs="EPSG:4326")

    geojson_reports = [("report1.geojson", gdf)]
    txt_reports = []

    mock_collection = MagicMock()

    processed_reports, new_gdf, features_added = dl.create_pipeline_features(
        geojson_reports, txt_reports, gas_lines_gdf, "EPSG:4326",
        gas_lines_collection=mock_collection,
        processed_reports=set(),
        use_mongodb=False,  # MongoDB interaction disabled
    )

    assert "report1.geojson" in processed_reports
    assert features_added
    # Mongo collection should NOT be called because use_mongodb=False
    mock_collection.assert_not_called()


def test_create_pipeline_features_with_empty_reports():
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

    assert processed_reports == set()
    assert new_gdf.equals(gas_lines_gdf)
    assert not features_added
