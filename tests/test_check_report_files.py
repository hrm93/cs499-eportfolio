import json
import tempfile
from pathlib import Path
import logging
import geopandas as gpd

from gis_tool.report_reader import find_new_reports, read_reports

# Logger configured to capture debug and info level messages during tests
logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)


def create_dummy_geojson_file(path: Path):
    """
    Create a minimal valid GeoJSON file with one Point feature for testing purposes.

    Parameters
    ----------
    path : Path
        The full file path where the dummy GeoJSON file will be created.
    """
    logger.info(f"Creating dummy GeoJSON file at {path}")

    # GeoJSON FeatureCollection with a single feature
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

    # Write the GeoJSON data to the specified file path
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    logger.info("Dummy GeoJSON file created successfully.")


def create_dummy_txt_file(path: Path):
    """
    Create a dummy TXT report file with a CSV header and one data line for testing.

    Parameters
    ----------
    path : Path
        The full file path where the dummy TXT file will be created.
    """
    logger.info(f"Creating dummy TXT file at {path}")

    # Write CSV header and a single data line
    with open(path, "w") as f:
        f.write("Name,Date,PSI,Material,Location,Notes\n")
        f.write('TestPipe2,2025-06-20,95,Plastic,"34.123, -118.456","Some notes"\n')
    logger.info("Dummy TXT file created successfully.")


def test_find_and_read_reports():
    """
    Integration test for report reading functions:

    - Creates temporary dummy .geojson and .txt report files.
    - Uses find_new_reports() to detect available report files.
    - Uses read_reports() to load GeoDataFrames from GeoJSON and list of dicts from TXT.
    - Validates that data loaded matches expected structure and contents.

    This test ensures that the report discovery and parsing pipeline
    handles both GeoJSON and TXT report formats correctly.
    """
    logger.info("Starting test_find_and_read_reports")

    # Use a temporary directory context to avoid polluting filesystem
    with tempfile.TemporaryDirectory() as tempdir:
        temp_path = Path(tempdir)

        # Create dummy report files in the temp directory
        geojson_path = temp_path / "dummy_report.geojson"
        txt_path = temp_path / "dummy_report.txt"
        create_dummy_geojson_file(geojson_path)
        create_dummy_txt_file(txt_path)

        # Find new reports in the temp directory using report_reader's function
        found_reports = find_new_reports(tempdir)
        logger.debug(f"Reports found: {found_reports}")

        # Assert both dummy files are detected
        assert "dummy_report.geojson" in found_reports
        assert "dummy_report.txt" in found_reports

        # Read the reports into GeoDataFrames (GeoJSON) and lists (TXT)
        geojson_reports, txt_reports = read_reports(found_reports, temp_path)

        # Validate GeoJSON reports contents
        assert len(geojson_reports) == 1
        report_name, gdf = geojson_reports[0]
        assert report_name == "dummy_report.geojson"
        assert isinstance(gdf, gpd.GeoDataFrame)
        assert not gdf.empty
        assert "Name" in gdf.columns
        assert gdf.iloc[0]["Name"] == "TestPipe1"

        # Validate TXT reports contents
        assert len(txt_reports) == 1
        report_name_txt, lines = txt_reports[0]
        assert report_name_txt == "dummy_report.txt"
        assert isinstance(lines, list)
        # Check that the expected record exists in the TXT lines list
        assert any(
            record.get("Name") == "TestPipe2" for record in lines), \
            "Expected record with Name 'TestPipe2' not found"

    logger.info("test_find_and_read_reports completed successfully.")


if __name__ == "__main__":
    import pytest
    # Setup logging for standalone runs
    logging.basicConfig(level=logging.DEBUG)
    # Run pytest programmatically on this file
    pytest.main([__file__])
