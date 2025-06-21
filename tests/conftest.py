### conftest.py

import logging
import os
import pytest
import geopandas as gpd
import pandas as pd

from geopandas import GeoDataFrame
from shapely.geometry import LineString, Point, Polygon
from pathlib import Path
from typing import Dict

from gis_tool.config import DEFAULT_CRS
from gis_tool.logger import setup_logging
from gis_tool import data_utils
from gis_tool import config

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs



# ---- FIXTURES ----

@pytest.fixture(scope="session", autouse=True)
def configure_logging():
    """
    Pytest fixture to set up logging for the test session.

    This fixture automatically initializes the logging configuration
    defined in the gis_tool.logger module before any tests run.
    """
    setup_logging()


@pytest.fixture(autouse=True)
def clear_env(monkeypatch, tmp_path):
    """
    Clears environment variables and provides a temporary config.ini for each test.
    """
    keys = [
        "MONGODB_URI",
        "DB_NAME",
        "DEFAULT_CRS",
        "DEFAULT_BUFFER_DISTANCE_FT",
        "LOG_FILENAME",
        "LOG_LEVEL",
        "MAX_WORKERS",
        "OUTPUT_FORMAT",
        "ALLOW_OVERWRITE_OUTPUT",
        "DRY_RUN_MODE",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)

    # Write a temporary config.ini file
    config_file = tmp_path / "config.ini"
    config_file.write_text("""
[DEFAULT]
output_format = shp
allow_overwrite_output = false
dry_run_mode = false
max_workers = 2

[DATABASE]
mongodb_uri = mongodb://localhost:27017/
db_name = test_db_ini

[SPATIAL]
default_crs = EPSG:32633
geographic_crs = EPSG:4326
buffer_layer_crs = EPSG:32633
default_buffer_distance_ft = 50.0

[LOGGING]
log_filename = test.log
log_level = debug

[OUTPUT]
output_format = geojson
""")

    monkeypatch.chdir(tmp_path)  # Switch to the tmp_path with config.ini
    yield


@pytest.fixture(autouse=True)
def patch_os_path(monkeypatch):
    """Patch os.path.isdir and os.path.isfile globally for all tests."""
    def mock_isdir(path: str) -> bool:
        logger.debug(f"Mock isdir check: {path}")
        if path in ['missing_input', 'missing_output_dir']:
            logger.warning(f"Directory missing: {path}")
            return False
        return True

    def mock_isfile(path: str) -> bool:
        logger.debug(f"Mock isfile check: {path}")
        if 'missing_report' in path:
            logger.warning(f"File missing: {path}")
            return False
        return True

    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)


@pytest.fixture
def sample_gdf():
    """
    Fixture that returns a sample GeoDataFrame with one point feature.

    The GeoDataFrame uses EPSG:32633 projected CRS suitable for metric buffering.
    """
    logger.debug("Creating sample GeoDataFrame fixture.")
    gdf = gpd.GeoDataFrame(
        {
            "Name": ["test"],
            "Date": ["2020-01-01"],
            "PSI": [100],
            "Material": ["steel"],
            "geometry": [Point(0, 0)]
        },
        crs="EPSG:32633"
    )
    logger.debug("Sample GeoDataFrame created.")
    return gdf


@pytest.fixture
def sample_gas_lines_gdf():
    """
        Pytest fixture that creates a sample GeoDataFrame containing LineString geometries
        representing gas lines for testing purposes.

        Returns
        -------
        geopandas.GeoDataFrame
            A GeoDataFrame with two LineString geometries and CRS set to EPSG:3857.
        """
    # Create a GeoDataFrame with LineStrings (simulating gas lines)
    logger.info("Creating sample gas lines GeoDataFrame for testing.")
    geoms = [LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])]
    gdf = gpd.GeoDataFrame({'geometry': geoms}, crs="EPSG:3857")
    logger.debug(f"Sample gas lines GeoDataFrame created with {len(gdf)} features.")
    return gdf

@pytest.fixture
def empty_gas_lines_gdf():
    """
    Provides an empty GeoDataFrame with the schema defined in dl.SCHEMA_FIELDS,
    suitable for testing functions that expect an empty gas lines dataset.
    """
    logger.debug("Created empty_gas_lines_gdf fixture.")
    return gpd.GeoDataFrame(columns=data_utils.SCHEMA_FIELDS, crs=DEFAULT_CRS)


@pytest.fixture
def sample_parks_gdf():
    """
    Pytest fixture that creates a sample GeoDataFrame containing Polygon geometries
    representing parks for testing purposes.

    Returns
    -------
    geopandas.GeoDataFrame
        A GeoDataFrame with one Polygon geometry and CRS set to EPSG:3857.
    """
    # Create a GeoDataFrame with Polygons (simulating parks)
    logger.info("Creating sample parks GeoDataFrame for testing.")
    geoms = [Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)])]
    gdf = gpd.GeoDataFrame({'geometry': geoms}, crs="EPSG:3857")
    logger.debug(f"Sample parks GeoDataFrame created with {len(gdf)} features.")
    return gdf


@pytest.fixture
def sample_future_development(tmp_path):
    """
    Fixture creating a simple Future Development shapefile with a point feature and CRS.

    Uses a temporary directory and yields the file path for test use.
    """
    logger.debug("Creating sample future development shapefile fixture.")
    gdf = gpd.GeoDataFrame(
        {"geometry": [Point(100, 100)]},
        crs="EPSG:32633"
    )
    path = tmp_path / "future_dev.shp"
    gdf.to_file(str(path))
    logger.debug(f"Sample future development shapefile created at {path}.")
    return path


@pytest.fixture
def sample_buffer(tmp_path):
    """
    Fixture creating a buffer shapefile with attributes and polygon geometry.

    Uses a temporary directory and yields the file path for test use.
    """
    logger.debug("Creating sample buffer shapefile fixture.")
    gdf = gpd.GeoDataFrame(
        {
            "Name": ["Buffer1"],
            "Date": ["2024-01-01"],
            "PSI": [120],
            "Material": ["steel"],
            "geometry": [Point(100, 100).buffer(10)]
        },
        crs="EPSG:32633"
    )
    path = tmp_path / "buffer.shp"
    gdf.to_file(str(path))
    logger.debug(f"Sample buffer shapefile created at {path}.")
    return path


@pytest.fixture
def sample_parks_file(tmp_path, sample_parks_gdf):
    """
       Pytest fixture that writes the sample parks GeoDataFrame to a temporary GeoJSON file.

       Parameters
       ----------
       tmp_path : pathlib.Path
           Temporary directory path provided by pytest for storing intermediate files.
       sample_parks_gdf : GeoDataFrame
           GeoDataFrame containing sample park geometries.

       Returns
       -------
       str
           File path to the created temporary GeoJSON file containing park geometries.
       """
    logger.info("Creating sample parks GeoJSON file for testing.")
    file_path = tmp_path / "parks.geojson"
    sample_parks_gdf.to_file(str(file_path), driver='GeoJSON')
    logger.debug(f"Sample parks GeoJSON written: {file_path}")
    return str(file_path)


@pytest.fixture
def sample_geojson_report():
    """
     Provides a sample GeoJSON report as a tuple of filename and GeoDataFrame.
     The GeoDataFrame contains a single feature with predefined attributes and geometry.
     """
    gdf = GeoDataFrame({
        'Name': ['line1'],
        'Date': [pd.Timestamp('2023-01-01')],
        'PSI': [150.0],
        'Material': ['steel'],
        "geometry": [Point(1.0, 2.0)]
    }, crs=DEFAULT_CRS)
    logger.debug("Created sample_geojson_report fixture with one feature.")
    return "report1.geojson", gdf


@pytest.fixture
def dummy_inputs(tmp_path: Path) -> Dict[str, str]:
    """Creates dummy input shapefiles and report file for pipeline tests."""
    input_folder = tmp_path / "reports"
    input_folder.mkdir()
    (input_folder / "dummy_report.txt").write_text("123\t456\t789\t12.34\t56.78\t90\t0\t1\n")

    def create_shapefile(path, coords):
        gdf = gpd.GeoDataFrame({"id": [1], "geometry": [LineString(coords)]}, crs="EPSG:26918")
        gdf.to_file(path)

    gas_lines_path = tmp_path / "gas_lines.shp"
    future_dev_path = tmp_path / "future_dev.shp"
    create_shapefile(gas_lines_path, [(0, 0), (1, 1)])
    create_shapefile(future_dev_path, [(2, 2), (3, 3)])

    return {
        "input_folder": str(input_folder),
        "gas_lines_path": str(gas_lines_path),
        "future_dev_path": str(future_dev_path),
        "output_path": str(tmp_path / "output.shp")
    }


@pytest.fixture
def dummy_geojson_output(tmp_path: Path, dummy_inputs: Dict[str, str]) -> Dict[str, str]:
    """Same as dummy_inputs but with .geojson output."""
    inputs = dummy_inputs.copy()
    inputs["output_path"] = str(tmp_path / "output.geojson")
    return inputs


@pytest.fixture
def tmp_reports_dir(tmp_path):
    """
    Fixture to create a temporary directory with sample report files:
    - A valid GeoJSON file
    - A valid TXT file
    - An unsupported file type for testing filtering

    Returns:
        pathlib.Path: Path to the temporary directory with sample files.
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


@pytest.fixture
def valid_txt_line():
    return "Line2,2023-03-01,200,copper,10.0,20.0"


@pytest.fixture(autouse=True)
def reset_config_yaml():
    config.config_yaml = None
