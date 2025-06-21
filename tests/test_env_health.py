# Test for checking environment compatibility and library readiness

import logging
from pathlib import Path

import fiona
import geopandas as gpd
import pyproj
import pytest
from shapely.geometry import Point

# Set up logger for consistent debug/info output
logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)  # Capture all log levels during tests


# ------------------------
# Tests for Third-Party Library Support
# ------------------------

def test_fiona():
    """
    Verify that Fiona has GeoJSON driver support enabled.

    This ensures the environment is configured with all necessary
    vector data format support, particularly for GeoJSON output.
    """
    logger.info("Starting test: Fiona supported drivers.")

    # Check whether 'GeoJSON' is included in Fiona's supported drivers
    assert "GeoJSON" in fiona.supported_drivers

    logger.info("Fiona supports GeoJSON driver.")


def test_pyproj():
    """
    Validate pyproj can create a CRS from EPSG:4326 and identify it correctly.

    This confirms that coordinate reference system handling is operational,
    particularly for WGS 84 (EPSG:4326), used in GeoJSON and web mapping.
    """
    logger.info("Starting test: pyproj EPSG lookup.")

    # Create CRS from EPSG code and verify it resolves back to 4326
    crs = pyproj.CRS.from_epsg(4326)
    assert crs.to_epsg() == 4326

    logger.info("pyproj EPSG:4326 test passed.")


def test_shapely():
    """
    Ensure Shapely can create and buffer geometries, and compute area.

    This test confirms that geometric operations are functional and spatial
    objects are valid after transformation.
    """
    logger.info("Starting test: Shapely buffer and area.")

    # Create a point and buffer it to create a circular polygon
    pt = Point(1, 2).buffer(1.0)
    logger.debug(f"Buffered geometry area: {pt.area}")

    # The area of a buffered point should be greater than zero
    assert pt.area > 0

    logger.info("Shapely buffer area test passed.")


# ------------------------
# Test for GeoPandas Data Loading
# ------------------------

def test_geopandas_local():
    """
    Confirm GeoPandas can read a shapefile and load it into a GeoDataFrame.

    This validates GeoPandas' ability to interface with local spatial files,
    which is critical for spatial analysis pipelines.
    """
    logger.info("Starting test: GeoPandas reading shapefile.")

    # Construct the path to a test shapefile located in the data directory
    base_dir = Path(__file__).parent.parent.resolve()
    path = base_dir / "data" / "ne_110m_populated_places.shp"
    logger.info(f"Testing GeoPandas file read from: {path}")

    # Skip test if shapefile does not exist (e.g., on clean environment)
    if not path.exists():
        logger.warning(f"Shapefile not found at {path}, skipping test.")
        pytest.skip(f"Shapefile not found at {path}")

    # Read the shapefile and verify it's not empty
    gdf = gpd.read_file(path)
    logger.debug(f"GeoDataFrame shape: {gdf.shape}")
    assert not gdf.empty

    logger.info("GeoPandas shapefile read test passed.")
