# tests/test_output_writer.py

import pytest
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from gis_tool import output_writer


def test_write_geojson(tmp_path):
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

    # Load back and check contents roughly
    gdf_loaded = gpd.read_file(str(output_file))
    assert len(gdf_loaded) == 2
    assert list(gdf_loaded['name']) == ['A', 'B']


def test_write_csv(tmp_path):
    # Create simple DataFrame
    df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })
    output_file = tmp_path / "test.csv"

    output_writer.write_csv(df, str(output_file))
    assert output_file.exists()

    # Read back and check contents
    df_loaded = pd.read_csv(str(output_file))
    assert list(df_loaded['col1']) == [1, 2, 3]
    assert list(df_loaded['col2']) == ['a', 'b', 'c']


def test_write_report(tmp_path):
    text = "This is a test report.\nLine 2."
    output_file = tmp_path / "report.txt"

    output_writer.write_report(text, str(output_file))
    assert output_file.exists()

    with open(output_file, "r", encoding="utf-8") as f:
        content = f.read()
    assert content == text


def test_write_geojson_invalid_path():
    gdf = gpd.GeoDataFrame({'name': ['A']},
                           geometry=[Point(0, 0)],
                           crs="EPSG:4326")

    # Invalid Windows path with illegal characters
    invalid_path = "C:\\invalid:folder\\test.geojson"

    with pytest.raises(FileNotFoundError):
        output_writer.write_geojson(gdf, invalid_path)


def test_write_csv_invalid_path():
    df = pd.DataFrame({'a': [1]})

    invalid_path = "C:\\invalid:folder\\test.csv"

    with pytest.raises(FileNotFoundError):
        output_writer.write_csv(df, invalid_path)


def test_write_report_invalid_path():
    invalid_path = "C:\\invalid:folder\\report.txt"

    with pytest.raises(FileNotFoundError):
        output_writer.write_report("test", invalid_path)