# Test for checking environment
import os
import logging

import fiona
import geopandas as gpd
import pyproj
from shapely.geometry import Point

logger = logging.getLogger("gis_tool")


# ------------------------
# Tests for Third-Party Library Support
# ------------------------

def test_fiona():
    """
    Test that Fiona supports GeoJSON driver.
    """
    logger.info("Starting test: Fiona supported drivers.")
    assert "GeoJSON" in fiona.supported_drivers
    logger.info("Fiona supports GeoJSON driver.")


def test_pyproj():
    """
    Test that pyproj correctly creates and identifies EPSG:4326 CRS.
    """
    logger.info("Starting test: pyproj EPSG lookup.")
    crs = pyproj.CRS.from_epsg(4326)
    assert crs.to_epsg() == 4326
    logger.info("pyproj EPSG:4326 test passed.")


def test_shapely():
    """
    Test that Shapely can create a buffered geometry and compute a valid area.
    """
    logger.info("Starting test: Shapely buffer and area.")
    pt = Point(1, 2).buffer(1.0)
    logger.debug(f"Buffered geometry area: {pt.area}")
    assert pt.area > 0
    logger.info("Shapely buffer area test passed.")


# ------------------------
# Test for GeoPandas Data Loading
# ------------------------

def test_geopandas_local():
    """
    Test that GeoPandas can read a local shapefile and it is not empty.
    """
    logger.info("Starting test: GeoPandas reading shapefile.")
    path = os.path.join("data", "ne_110m_populated_places.shp")
    gdf = gpd.read_file(path)
    logger.debug(f"GeoDataFrame shape: {gdf.shape}")
    assert not gdf.empty
    logger.info("GeoPandas shapefile read test passed.")
