# test_report_reader
import logging
from unittest.mock import patch

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from gis_tool import report_reader as rr
from gis_tool.report_reader import find_new_reports, load_geojson_report, load_txt_report_lines

logger = logging.getLogger("gis_tool")


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


def test_find_new_reports_nonexistent_folder(caplog):
    """
    Test that find_new_reports handles a nonexistent folder gracefully,
    logs an error, and returns an empty list.
    """
    logger.info("Testing find_new_reports with nonexistent folder.")
    caplog.set_level("ERROR")
    result = rr.find_new_reports("nonexistent_folder_xyz")
    logger.debug(f"Result for nonexistent folder: {result}")
    assert result == []
    assert any("does not exist" in rec.message for rec in caplog.records)
    logger.info("test_find_new_reports_nonexistent_folder passed.")


def test_load_txt_report_lines(tmp_path):
    """
    Test load_txt_report_lines function to ensure:
    - It correctly reads header and data lines from a text report file.
    - It ignores blank lines.
    - It returns an empty list if the file is missing.
    """
    logger.info("Testing load_txt_report_lines function.")

    # Create a test file with header, data lines, and blank lines
    file_path = tmp_path / "report.txt"
    content = (
        "Id Name X Y Date PSI Material\n"
        "1 Line1 10.0 20.0 2023-01-01 250 steel\n"
        "Line 2\n"
        "\n"  # blank line to test ignoring blank lines
        "Line 3\n"
    )
    file_path.write_text(content)

    # Load lines from the created file
    lines = load_txt_report_lines(str(file_path))
    logger.debug(f"Loaded lines from TXT report: {lines}")

    # Check that blank lines are ignored and lines are read correctly
    assert len(lines) == 4  # Header + 3 data lines (Line1, Line 2, Line 3)
    assert "Line1" in lines[1]
    assert "Line 2" in lines[2]
    assert "Line 3" in lines[3]

    # Test behavior when file is missing
    missing_path = tmp_path / "nofile.txt"
    lines_missing = load_txt_report_lines(str(missing_path))
    logger.debug(f"Lines loaded from non-existent file: {lines_missing}")
    assert lines_missing == []

    logger.info("test_load_txt_report_lines passed.")


def test_load_geojson_report(tmp_reports_dir):
    """
    Test that load_geojson_report reads a GeoJSON file into a GeoDataFrame
    and applies the correct target CRS.
    """
    logger.info("Testing load_geojson_report function.")
    geojson_file = tmp_reports_dir / "test.geojson"
    target_crs = "EPSG:4326"

    gdf = rr.load_geojson_report(geojson_file, target_crs)
    logger.debug(f"GeoDataFrame loaded with CRS: {gdf.crs}")
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_string() == target_crs
    logger.info("test_load_geojson_report passed.")


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


@patch("geopandas.read_file")
def test_load_geojson_report_transforms_crs(mock_read_file, tmp_reports_dir):
    """
    Test that load_geojson_report correctly transforms CRS when the source CRS
    differs from the target CRS, using a mocked GeoDataFrame.
    """
    logger.info("Testing load_geojson_report with CRS transformation.")
    gdf_mock = gpd.GeoDataFrame(
        geometry=[Point(0, 0)],
        crs="EPSG:3857"
    )
    mock_read_file.return_value = gdf_mock

    target_crs = "EPSG:4326"
    result = rr.load_geojson_report(tmp_reports_dir / "test.geojson", target_crs)

    logger.debug(f"Mock GeoDataFrame CRS: {gdf_mock.crs}, Result CRS: {result.crs}")
    assert result.crs.to_string() == target_crs
    mock_read_file.assert_called_once()
    logger.info("test_load_geojson_report_transforms_crs passed.")


def test_read_reports(tmp_reports_dir):
    """
    Test that read_reports returns correct GeoJSON and TXT reports
    and excludes unsupported file types.
    """
    logger.info("Testing read_reports function.")
    reports = ["test.geojson", "test.txt", "bad.xyz"]

    geojson_reports, txt_reports = rr.read_reports(reports, tmp_reports_dir)
    logger.debug(f"GeoJSON reports returned: {[r[0] for r in geojson_reports]}")
    logger.debug(f"TXT reports returned: {[r[0] for r in txt_reports]}")

    assert any(r[0] == "test.geojson" and isinstance(r[1], gpd.GeoDataFrame) for r in geojson_reports)
    assert any(r[0] == "test.txt" and isinstance(r[1], list) for r in txt_reports)

    unsupported_names = [r[0] for r in geojson_reports] + [r[0] for r in txt_reports]
    assert "bad.xyz" not in unsupported_names
    logger.info("test_read_reports passed.")


def test_read_reports_logs_warnings_and_errors(tmp_reports_dir, caplog):
    """
    Test that read_reports logs warnings for unsupported file types.
    """
    logger.info("Testing read_reports logs for unsupported file types.")
    caplog.set_level("WARNING")

    reports = ["bad.xyz"]
    geojson_reports, txt_reports = rr.read_reports(reports, tmp_reports_dir)

    assert any("Unsupported report type" in rec.message for rec in caplog.records)
    logger.info("test_read_reports_logs_warnings_and_errors passed.")
