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

logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)  # Set logger to DEBUG level for detailed output


# ---- FIXTURES ----

@pytest.fixture(scope="session", autouse=True)
def configure_logging():
    """
    Session-scoped fixture to set up logging configuration once per test session.

    Automatically invokes the logging setup function defined in gis_tool.logger
    so all tests produce consistent, formatted log output at the DEBUG level.
    """
    setup_logging()


@pytest.fixture(autouse=True)
def clear_env(monkeypatch, tmp_path):
    """
    Fixture that clears selected environment variables and sets up a temporary config.ini file.

    This ensures environment-dependent variables do not persist across tests,
    avoiding contamination or unexpected behavior.

    It writes a realistic config.ini to the temporary directory and
    changes the working directory to it, so tests can read from config.ini.

    Environment variables cleared include MongoDB URI, DB name, CRS settings,
    logging config, max workers, output format, and dry-run mode.
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

    # Create config.ini file in temporary path for test isolation
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

    # Change current working directory so config file is discoverable
    monkeypatch.chdir(tmp_path)
    yield


@pytest.fixture(autouse=True)
def patch_os_path(monkeypatch):
    """
    Globally patch os.path.isdir and os.path.isfile functions for all tests.

    Mocks directory and file existence checks to simulate missing or present paths
    without touching the real filesystem.

    - Paths 'missing_input' and 'missing_output_dir' will simulate missing directories.
    - Paths containing 'missing_report' simulate missing files.

    This helps test error handling related to filesystem operations.
    """
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
    Fixture returning a sample GeoDataFrame with a single Point feature.

    The GeoDataFrame has columns typical of your spatial data and uses EPSG:32633,
    a projected CRS suitable for buffering and distance operations.

    Returns:
        geopandas.GeoDataFrame: Sample point geometry with attributes.
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
    Fixture creating a GeoDataFrame with LineString geometries representing gas lines.

    This simulates a dataset of gas pipeline lines, useful for buffer and overlay tests.

    Returns:
        geopandas.GeoDataFrame: Contains two LineString geometries with CRS EPSG:3857.
    """
    logger.info("Creating sample gas lines GeoDataFrame for testing.")
    geoms = [LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])]
    gdf = gpd.GeoDataFrame({'geometry': geoms}, crs="EPSG:3857")
    logger.debug(f"Sample gas lines GeoDataFrame created with {len(gdf)} features.")
    return gdf


@pytest.fixture
def empty_gas_lines_gdf():
    """
    Fixture providing an empty GeoDataFrame with the schema defined in data_utils.SCHEMA_FIELDS.

    Useful for testing behaviors when no gas line features exist.

    Returns:
        geopandas.GeoDataFrame: Empty GeoDataFrame with correct columns and default CRS.
    """
    logger.debug("Created empty_gas_lines_gdf fixture.")
    return gpd.GeoDataFrame(columns=data_utils.SCHEMA_FIELDS, crs=DEFAULT_CRS)


@pytest.fixture
def sample_parks_gdf():
    """
    Fixture that creates a GeoDataFrame with Polygon geometries representing parks.

    Useful for spatial intersection or difference operations in tests.

    Returns:
        geopandas.GeoDataFrame: One Polygon feature with CRS EPSG:3857.
    """
    logger.info("Creating sample parks GeoDataFrame for testing.")
    geoms = [Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)])]
    gdf = gpd.GeoDataFrame({'geometry': geoms}, crs="EPSG:3857")
    logger.debug(f"Sample parks GeoDataFrame created with {len(gdf)} features.")
    return gdf


@pytest.fixture
def sample_future_development(tmp_path):
    """
    Creates a sample shapefile representing future development areas with one point feature.

    The shapefile is written to a temporary directory and the path is returned for test use.

    Args:
        tmp_path (pathlib.Path): Pytest-provided temporary directory.

    Returns:
        pathlib.Path: Path to the created shapefile.
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
    Creates a sample buffer shapefile with polygon geometry and attribute data.

    The shapefile is stored in a temporary directory, useful for buffer processing tests.

    Args:
        tmp_path (pathlib.Path): Temporary directory for file creation.

    Returns:
        pathlib.Path: Path to the created buffer shapefile.
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
    Writes the sample parks GeoDataFrame to a temporary GeoJSON file.

    Args:
        tmp_path (pathlib.Path): Temporary directory path.
        sample_parks_gdf (GeoDataFrame): GeoDataFrame with parks polygons.

    Returns:
        str: File path to the created GeoJSON file.
    """
    logger.info("Creating sample parks GeoJSON file for testing.")
    file_path = tmp_path / "parks.geojson"
    sample_parks_gdf.to_file(str(file_path), driver='GeoJSON')
    logger.debug(f"Sample parks GeoJSON written: {file_path}")
    return str(file_path)


@pytest.fixture
def sample_geojson_report():
    """
    Provides a sample GeoJSON report as a tuple containing filename and GeoDataFrame.

    The GeoDataFrame contains a single point feature with relevant attributes,
    suitable for testing report parsing or reading functionality.

    Returns:
        tuple: (filename: str, GeoDataFrame)
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
    """
    Creates dummy input files and shapefiles to simulate pipeline inputs.

    - Creates a directory for reports with a dummy report text file.
    - Creates shapefiles for gas lines and future development.

    Args:
        tmp_path (pathlib.Path): Temporary directory for file creation.

    Returns:
        dict: Paths for 'input_folder', 'gas_lines_path', 'future_dev_path', and 'output_path'.
    """
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
    """
    Provides dummy inputs similar to dummy_inputs but modifies output_path for GeoJSON output.

    Args:
        tmp_path (pathlib.Path): Temporary directory path.
        dummy_inputs (dict): Dictionary of dummy input paths.

    Returns:
        dict: Modified dummy inputs with GeoJSON output path.
    """
    inputs = dummy_inputs.copy()
    inputs["output_path"] = str(tmp_path / "output.geojson")
    return inputs


@pytest.fixture
def tmp_reports_dir(tmp_path):
    """
    Creates a temporary directory populated with sample report files of varying formats.

    Includes:
    - A valid GeoJSON file
    - A valid TXT file
    - An unsupported file type to test filtering logic

    Args:
        tmp_path (pathlib.Path): Temporary directory.

    Returns:
        pathlib.Path: Path to the directory with sample files.
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
    """
    Provides a valid CSV-style text line for testing text report parsing.

    Returns:
        str: Sample line mimicking a report record.
    """
    return "Line2,2023-03-01,200,copper,10.0,20.0"


@pytest.fixture(autouse=True)
def reset_config_yaml():
    """
    Auto-use fixture to reset the global config_yaml cache before each test.

    Ensures no cached configuration persists between tests, enforcing fresh reads.
    """
    config.config_yaml = None
