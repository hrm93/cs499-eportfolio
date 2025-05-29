# tests/test_output_writer.py
import logging

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from gis_tool import output_writer


logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


def test_write_geojson(tmp_path):
    """
    Test writing a GeoDataFrame to a GeoJSON file and verify
    that the file is created and content matches the input data.
    """
    logger.info("Starting test_write_geojson")

    # Create simple GeoDataFrame
    gdf = gpd.GeoDataFrame(
        {'name': ['A', 'B']},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326"
    )
    output_file = tmp_path / "test.geojson"

    # Call write_geojson and check file creation
    output_writer.write_geojson(gdf, str(output_file))
    assert output_file.exists()
    logger.debug(f"GeoJSON file created at {output_file}")

    # Load back and check contents roughly
    gdf_loaded = gpd.read_file(str(output_file))
    assert len(gdf_loaded) == 2
    assert list(gdf_loaded['name']) == ['A', 'B']
    logger.info("test_write_geojson passed successfully")


def test_write_csv(tmp_path):
    """
    Test writing a DataFrame to CSV and verify the file creation
    and content accuracy after reloading.
    """
    logger.info("Starting test_write_csv")

    # Create simple DataFrame
    df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })
    output_file = tmp_path / "test.csv"

    output_writer.write_csv(df, str(output_file))
    assert output_file.exists()
    logger.debug(f"CSV file created at {output_file}")

    # Read back and check contents
    df_loaded = pd.read_csv(str(output_file))
    assert list(df_loaded['col1']) == [1, 2, 3]
    assert list(df_loaded['col2']) == ['a', 'b', 'c']
    logger.info("test_write_csv passed successfully")


def test_write_report(tmp_path):
    """
    Test writing a plain text report to a .txt file and verify
    file creation and content.
    """
    logger.info("Starting test_write_report")

    text = "This is a test report.\nLine 2."
    output_file = tmp_path / "report.txt"

    output_writer.write_report(text, str(output_file))
    assert output_file.exists()
    logger.debug(f"Report file created at {output_file}")

    with open(output_file, "r", encoding="utf-8") as f:
        content = f.read()
    assert content == text
    logger.info("test_write_report passed successfully")


def test_write_geojson_invalid_path():
    """
    Test write_geojson with an invalid file path to ensure
    proper exception (FileNotFoundError) is raised.
    """
    logger.info("Starting test_write_geojson_invalid_path")

    gdf = gpd.GeoDataFrame({'name': ['A']},
                           geometry=[Point(0, 0)],
                           crs="EPSG:4326")

    invalid_path = "C:\\invalid:folder\\test.geojson"

    with pytest.raises(FileNotFoundError):
        output_writer.write_geojson(gdf, invalid_path)
    logger.info("test_write_geojson_invalid_path passed successfully")


def test_write_csv_invalid_path():
    """
    Test write_csv with an invalid file path to verify
    FileNotFoundError is raised.
    """
    logger.info("Starting test_write_csv_invalid_path")

    df = pd.DataFrame({'a': [1]})

    invalid_path = "C:\\invalid:folder\\test.csv"

    with pytest.raises(FileNotFoundError):
        output_writer.write_csv(df, invalid_path)
    logger.info("test_write_csv_invalid_path passed successfully")


def test_write_report_invalid_path():
    """
    Test write_report with an invalid path to ensure
    FileNotFoundError is raised.
    """
    logger.info("Starting test_write_report_invalid_path")

    invalid_path = "C:\\invalid:folder\\report.txt"

    with pytest.raises(FileNotFoundError):
        output_writer.write_report("test", invalid_path)
    logger.info("test_write_report_invalid_path passed successfully")
