# test_report_reader
import logging
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point

from gis_tool import report_reader as rr

logger = logging.getLogger("gis_tool")


def test_find_new_reports(tmp_reports_dir):
    """
    Test that find_new_reports correctly identifies supported report files
    and excludes unsupported file types.
    """
    logger.info("Testing find_new_reports function.")
    found = rr.find_new_reports(str(tmp_reports_dir))
    logger.debug(f"Reports found: {found}")
    assert "test.geojson" in found
    assert "test.txt" in found
    assert "bad.xyz" not in found
    logger.info("test_find_new_reports passed.")


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


def test_load_txt_report_lines(tmp_reports_dir):
    """
    Test that load_txt_report_lines correctly reads lines from a text report,
    ignoring blank lines, and returns an empty list if file is missing.
    """
    logger.info("Testing load_txt_report_lines function.")
    txt_file = tmp_reports_dir / "test.txt"
    lines = rr.load_txt_report_lines(str(txt_file))
    logger.debug(f"Lines loaded from txt: {lines}")
    assert lines == ["Line 1", "Line 2", "Line 3"]

    lines_missing = rr.load_txt_report_lines(str(tmp_reports_dir / "nofile.txt"))
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
