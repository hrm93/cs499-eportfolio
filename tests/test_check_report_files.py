import os
import json
import tempfile
from pathlib import Path
import logging

import geopandas as gpd
from shapely.geometry import Point

from gis_tool.report_reader import find_new_reports, read_reports

logger = logging.getLogger(__name__)


def create_dummy_geojson_file(path: Path):
    """
    Create a minimal valid GeoJSON file with one point feature.

    Args:
        path (Path): The file path where to create the dummy GeoJSON.
    """
    logger.info(f"Creating dummy GeoJSON file at {path}")
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "Name": "TestPipe1",
                    "Date": "2025-05-30",
                    "PSI": 100,
                    "Material": "steel"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [100.0, 0.0]
                }
            }
        ]
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    logger.info("Dummy GeoJSON file created successfully.")


def create_dummy_txt_file(path: Path):
    """
    Create a dummy TXT report file with header and one data line.

    Args:
        path (Path): The file path where to create the dummy TXT file.
    """
    logger.info(f"Creating dummy TXT file at {path}")
    lines = [
        "Id Name X Y Date PSI Material",
        "1 TestPipe2 101.0 1.0 2025-05-30 120 steel"
    ]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info("Dummy TXT file created successfully.")


def test_find_and_read_reports():
    """
    Test that find_new_reports identifies dummy report files and read_reports
    loads their contents correctly.

    This test:
    - Creates dummy .geojson and .txt files in a temporary directory.
    - Uses find_new_reports to detect those files.
    - Uses read_reports to load GeoDataFrames and text lines.
    - Checks the contents and types of loaded data.
    """
    logger.info("Starting test_find_and_read_reports")
    with tempfile.TemporaryDirectory() as tempdir:
        temp_path = Path(tempdir)

        # Create dummy files
        geojson_path = temp_path / "dummy_report.geojson"
        txt_path = temp_path / "dummy_report.txt"

        create_dummy_geojson_file(geojson_path)
        create_dummy_txt_file(txt_path)

        # Use find_new_reports to find these files
        found_reports = find_new_reports(tempdir)
        logger.debug(f"Reports found: {found_reports}")
        assert "dummy_report.geojson" in found_reports
        assert "dummy_report.txt" in found_reports

        # Read the reports using read_reports
        geojson_reports, txt_reports = read_reports(found_reports, temp_path)

        # Check GeoJSON reports read correctly
        assert len(geojson_reports) == 1
        report_name, gdf = geojson_reports[0]
        assert report_name == "dummy_report.geojson"
        assert isinstance(gdf, gpd.GeoDataFrame)
        assert not gdf.empty
        assert "Name" in gdf.columns
        assert gdf.iloc[0]["Name"] == "TestPipe1"

        # Check TXT reports read correctly
        assert len(txt_reports) == 1
        report_name_txt, lines = txt_reports[0]
        assert report_name_txt == "dummy_report.txt"
        assert isinstance(lines, list)
        assert any("TestPipe2" in line for line in lines)
    logger.info("test_find_and_read_reports completed successfully.")


if __name__ == "__main__":
    import pytest
    logging.basicConfig(level=logging.DEBUG)
    pytest.main([__file__])
