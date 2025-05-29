# test_report_reader
import pytest
from unittest.mock import patch
import geopandas as gpd
from shapely.geometry import Point
from gis_tool import report_reader as rr
import logging

logger = logging.getLogger("gis_tool")


@pytest.fixture
def tmp_reports_dir(tmp_path):
    """
    Fixture to create a temporary directory with sample report files:
    - A valid GeoJSON file
    - A valid TXT file
    - An unsupported file type for testing filtering
    """
    logger.info("Setting up temporary reports directory with sample files.")

    geojson_path = tmp_path / "test.geojson"
    txt_path = tmp_path / "test.txt"
    bad_file = tmp_path / "bad.xyz"

    geojson_content = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "Point",
                "coordinates": [0.0, 0.0]
            }
        }]
    }
    import json
    geojson_path.write_text(json.dumps(geojson_content))
    logger.debug(f"Wrote GeoJSON file at {geojson_path}")

    txt_path.write_text("Line 1\nLine 2\n\nLine 3\n")
    logger.debug(f"Wrote TXT file at {txt_path}")

    bad_file.write_text("Unsupported file content")
    logger.debug(f"Wrote unsupported file at {bad_file}")

    logger.info("Temporary reports directory setup complete.")
    return tmp_path


def test_find_new_reports(tmp_reports_dir):
    logger.info("Testing find_new_reports function.")
    found = rr.find_new_reports(str(tmp_reports_dir))
    logger.debug(f"Reports found: {found}")
    assert "test.geojson" in found
    assert "test.txt" in found
    assert "bad.xyz" not in found
    logger.info("test_find_new_reports passed.")


def test_load_txt_report_lines(tmp_reports_dir):
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
    logger.info("Testing load_geojson_report function.")
    geojson_file = tmp_reports_dir / "test.geojson"
    target_crs = "EPSG:4326"

    gdf = rr.load_geojson_report(geojson_file, target_crs)
    logger.debug(f"GeoDataFrame loaded with CRS: {gdf.crs}")
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_string() == target_crs
    logger.info("test_load_geojson_report passed.")


def test_read_reports(tmp_reports_dir):
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


@patch("geopandas.read_file")
def test_load_geojson_report_transforms_crs(mock_read_file, tmp_reports_dir):
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


def test_find_new_reports_nonexistent_folder(caplog):
    logger.info("Testing find_new_reports with nonexistent folder.")
    caplog.set_level("ERROR")
    result = rr.find_new_reports("nonexistent_folder_xyz")
    logger.debug(f"Result for nonexistent folder: {result}")
    assert result == []
    assert any("does not exist" in rec.message for rec in caplog.records)
    logger.info("test_find_new_reports_nonexistent_folder passed.")


def test_read_reports_logs_warnings_and_errors(tmp_reports_dir, caplog):
    logger.info("Testing read_reports logs for unsupported file types.")
    caplog.set_level("WARNING")

    reports = ["bad.xyz"]
    geojson_reports, txt_reports = rr.read_reports(reports, tmp_reports_dir)

    assert any("Unsupported report type" in rec.message for rec in caplog.records)
    logger.info("test_read_reports_logs_warnings_and_errors passed.")
