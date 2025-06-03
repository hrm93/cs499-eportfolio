# test spatial_utils
import os
import tempfile
import logging

import geopandas as gpd
from shapely.geometry import Point


from gis_tool.spatial_utils import ensure_projected_crs
from gis_tool import config
from gis_tool.buffer_processor import create_buffer_with_geopandas, merge_buffers_into_planning_file

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs



def test_ensure_projected_crs_already_projected():
    """
    Test that a projected CRS is returned unchanged by ensure_projected_crs.
    """
    logger.info("Running test_ensure_projected_crs_already_projected")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=config.DEFAULT_CRS)
    projected = ensure_projected_crs(gdf)
    logger.debug(f"Original CRS: {gdf.crs}, Projected CRS: {projected.crs}")
    assert projected.crs.to_string() == config.DEFAULT_CRS
    logger.info("test_ensure_projected_crs_already_projected passed.")


def test_ensure_projected_crs_needs_reproject():
    """
    Test that a geographic CRS (EPSG:4326) is reprojected by ensure_projected_crs.
    """
    logger.info("Running test_ensure_projected_crs_needs_reproject")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=config.GEOGRAPHIC_CRS)
    projected = ensure_projected_crs(gdf)
    logger.debug(f"Original CRS: {gdf.crs}, Projected CRS: {projected.crs}")
    assert projected.crs != gdf.crs
    assert projected.crs.is_projected
    logger.info("test_ensure_projected_crs_needs_reproject passed.")


def test_create_buffer_with_missing_crs():
    """
    Test create_buffer_with_geopandas handles input files missing CRS by assigning default.
    """
    logger.info("Running test_create_buffer_with_missing_crs")
    with tempfile.TemporaryDirectory() as tmpdir:
        gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=config.GEOGRAPHIC_CRS)
        input_path = os.path.join(tmpdir, "no_crs_input.shp")
        gdf.to_file(input_path)
        logger.debug(f"Created shapefile with missing CRS at {input_path}")

        buffered_gdf = create_buffer_with_geopandas(input_path, buffer_distance_ft=config.DEFAULT_BUFFER_DISTANCE_FT)

        logger.debug(f"Buffered GeoDataFrame CRS: {buffered_gdf.crs}")
        assert buffered_gdf.crs is not None
        assert not buffered_gdf.empty
    logger.info("test_create_buffer_with_missing_crs passed.")


def test_merge_missing_crs_inputs(tmp_path):
    """
    Test merge behavior when one or both files lack a CRS.
    """
    logger.info("Running test_merge_missing_crs_inputs")
    buffer = gpd.GeoDataFrame(geometry=[Point(5, 5).buffer(5)], crs="EPSG:4326")
    future_dev = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:4326")

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"
    buffer.to_file(str(buffer_fp))
    future_dev.to_file(str(future_fp))
    logger.debug(f"Buffer and future development shapefiles saved at {buffer_fp} and {future_fp}")

    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)
    result = gpd.read_file(future_fp)

    logger.debug(f"Merged shapefile CRS: {result.crs}, feature count: {len(result)}")
    assert not result.empty
    assert result.crs is not None
    logger.info("test_merge_missing_crs_inputs passed.")
