### conftest.py

import logging
import pytest
import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


@pytest.fixture(autouse=True)
def configure_logging():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')


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
