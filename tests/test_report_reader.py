import logging
from unittest.mock import patch

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from gis_tool import report_reader as rr

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)


@pytest.mark.parametrize("files, expected", [
    (["test.txt", "test.geojson", "ignore.csv"], ["test.txt", "test.geojson"]),
    ([], []),
])
def test_find_new_reports(tmp_path, files, expected):
    """
    Test that find_new_reports identifies .txt and .geojson files correctly,
    ignoring other extensions, and returns the expected list.

    Args:
        tmp_path (Path): pytest tmp_path fixture.
        files (list[str]): Filenames to create for testing.
        expected (list[str]): Expected files to be found.
    """
    logger.info("Starting test_find_new_reports")

    # Create test files in tmp_path
    for filename in files:
        path = tmp_path / filename
        if not path.exists():
            path.write_text("Dummy content")

    # Call function under test
    result = rr.find_new_reports(str(tmp_path))
    logger.debug(f"find_new_reports found: {result}")

    # Assert that only .txt and .geojson files are returned
    assert sorted(result) == sorted(expected)

    logger.info("Completed test_find_new_reports")


def test_find_new_reports_nonexistent_folder(caplog):
    """
    Test that find_new_reports returns an empty list and logs an error
    if the input folder does not exist.

    Args:
        caplog: pytest logging capture fixture.
    """
    logger.info("Starting test_find_new_reports_nonexistent_folder")
    caplog.set_level("ERROR")

    # Call with a folder path that does not exist
    result = rr.find_new_reports("nonexistent_folder_xyz")
    logger.debug(f"Result for nonexistent folder: {result}")

    # Expect empty list return
    assert result == []
    # Check error logged about folder not existing
    assert any("does not exist" in rec.message for rec in caplog.records)

    logger.info("Completed test_find_new_reports_nonexistent_folder")


def test_load_txt_report_lines_reads_correctly(tmp_path):
    """
    Test load_txt_report_lines reads all non-empty lines and ignores blank lines.

    Args:
        tmp_path (Path): pytest tmp_path fixture.
    """
    logger.info("Starting test_load_txt_report_lines_reads_correctly")

    file_path = tmp_path / "test.txt"
    content = (
        "Id Name X Y Date PSI Material\n"  # header line
        "1 Line1 10.0 20.0 2023-01-01 250 steel\n"  # data line
        "\n"  # blank line should be ignored
        "Line 2\n"  # additional data line
        "Line 3\n"  # additional data line
    )
    # Write content to file
    file_path.write_text(content)

    # Read lines with function under test
    lines = rr.load_txt_report_lines(str(file_path))
    logger.debug(f"Loaded lines: {lines}")

    # Expect blank lines ignored, so 4 lines returned (header + 3 data)
    assert len(lines) == 4
    # Confirm data lines contain expected content
    assert "Line1" in lines[1]
    assert "Line 2" in lines[2]
    assert "Line 3" in lines[3]

    logger.info("Completed test_load_txt_report_lines_reads_correctly")


def test_load_txt_report_lines_missing_file(tmp_path):
    """
    Test load_txt_report_lines returns empty list for missing files.

    Args:
        tmp_path (Path): pytest tmp_path fixture.
    """
    logger.info("Starting test_load_txt_report_lines_missing_file")

    missing_path = tmp_path / "nofile.txt"
    # Call with non-existent file path
    lines = rr.load_txt_report_lines(str(missing_path))

    logger.debug(f"Lines loaded from missing file: {lines}")
    # Expect empty list, no crash
    assert lines == []

    logger.info("Completed test_load_txt_report_lines_missing_file")


def test_load_geojson_report_returns_gdf(tmp_reports_dir):
    """
    Test load_geojson_report reads a GeoJSON file and returns a GeoDataFrame with
    the expected CRS.

    Args:
        tmp_reports_dir (Path): Fixture directory containing a GeoJSON file.
    """
    logger.info("Starting test_load_geojson_report_returns_gdf")

    geojson_file = tmp_reports_dir / "test.geojson"
    target_crs = "EPSG:4326"

    # Call function to load GeoJSON
    gdf = rr.load_geojson_report(geojson_file, target_crs)
    logger.debug(f"GeoDataFrame CRS: {gdf.crs}")

    # Confirm output is GeoDataFrame and CRS matches target
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_string() == target_crs

    logger.info("Completed test_load_geojson_report_returns_gdf")


def test_load_geojson_report_reprojects(tmp_path):
    """
    Test load_geojson_report reprojects GeoDataFrame if CRS does not match target.

    Args:
        tmp_path (Path): pytest tmp_path fixture.
    """
    logger.info("Starting test_load_geojson_report_reprojects")

    # Create GeoDataFrame with different CRS
    gdf = gpd.GeoDataFrame(
        {"Name": ["LineA"], "Date": [pd.Timestamp("2024-01-01")], "PSI": [100.0], "Material": ["steel"], "geometry": [Point(1, 1)]},
        crs="EPSG:3857"
    )
    file_path = tmp_path / "test.geojson"
    # Save GeoDataFrame as GeoJSON
    gdf.to_file(str(file_path), driver="GeoJSON")

    # Load and expect reprojection to EPSG:4326
    result_gdf = rr.load_geojson_report(file_path, "EPSG:4326")
    logger.debug(f"Reprojected GeoDataFrame CRS: {result_gdf.crs.to_string()}")

    assert result_gdf.crs.to_string() == "EPSG:4326"
    assert "Name" in result_gdf.columns

    logger.info("Completed test_load_geojson_report_reprojects")


@patch("geopandas.read_file")
def test_load_geojson_report_transforms_crs(mock_read_file, tmp_reports_dir):
    """
    Test load_geojson_report transforms CRS when the original GeoDataFrame
    has a different CRS than the target CRS.

    Args:
        mock_read_file (MagicMock): Mock for geopandas.read_file.
        tmp_reports_dir (Path): Directory path containing test files.
    """
    logger.info("Starting test_load_geojson_report_transforms_crs")

    # Mock GeoDataFrame with a specific CRS
    gdf_mock = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:3857")
    mock_read_file.return_value = gdf_mock

    target_crs = "EPSG:4326"
    # Call function, expecting reprojection
    result = rr.load_geojson_report(tmp_reports_dir / "test.geojson", target_crs)

    logger.debug(f"Mock CRS: {gdf_mock.crs}, Result CRS: {result.crs}")

    # Assert CRS was transformed to target
    assert result.crs.to_string() == target_crs
    mock_read_file.assert_called_once()

    logger.info("Completed test_load_geojson_report_transforms_crs")


def test_read_reports_reads_and_skips(tmp_reports_dir, caplog):
    """
    Test that read_reports correctly reads GeoJSON and TXT reports,
    skips unsupported file types, and logs appropriate warnings.

    Args:
        tmp_reports_dir (Path): Directory path containing test files.
        caplog: pytest logging capture fixture.
    """
    logger.info("Starting test_read_reports_reads_and_skips")
    caplog.set_level("WARNING")

    reports = ["test.geojson", "test.txt", "bad.xyz"]
    # Call the function under test
    geojson_reports, txt_reports = rr.read_reports(reports, tmp_reports_dir)

    logger.debug(f"GeoJSON reports: {[r[0] for r in geojson_reports]}")
    logger.debug(f"TXT reports: {[r[0] for r in txt_reports]}")

    # Check that geojson and txt reports are returned correctly
    assert any(r[0] == "test.geojson" and isinstance(r[1], gpd.GeoDataFrame) for r in geojson_reports)
    assert any(r[0] == "test.txt" and isinstance(r[1], list) for r in txt_reports)

    # Unsupported file should be skipped
    unsupported_names = [r[0] for r in geojson_reports] + [r[0] for r in txt_reports]
    assert "bad.xyz" not in unsupported_names

    # Confirm warning logged about unsupported report type
    assert any("Unsupported report type" in rec.message for rec in caplog.records)

    logger.info("Completed test_read_reports_reads_and_skips")
