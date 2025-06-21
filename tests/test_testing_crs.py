import logging
import geopandas as gpd
import pytest

logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)

def test_load_parks_shapefile_and_crs():
    """
    Test loading the parks shapefile and verify that its CRS is correctly detected
    and matches the expected CRS.
    """
    parks_path = r"C:\Users\xrose\PycharmProjects\PythonProject\data\shapefiles\parks.shp"
    expected_crs = "EPSG:32633"

    try:
        parks_gdf = gpd.read_file(parks_path)
        logger.info(f"Loaded parks shapefile from {parks_path}")
    except Exception as e:
        logger.error(f"Failed to load parks shapefile: {e}")
        pytest.fail(f"Could not load parks shapefile: {e}")

    # Assert CRS is detected
    assert parks_gdf.crs is not None, "CRS should not be None"
    logger.info(f"CRS detected: {parks_gdf.crs}")

    # Assert CRS matches expected CRS
    actual_crs_str = parks_gdf.crs.to_string()
    assert actual_crs_str == expected_crs, f"Expected CRS {expected_crs}, but got {actual_crs_str}"
    logger.info(f"CRS matches expected: {actual_crs_str}")
