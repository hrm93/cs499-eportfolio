# test_spatial_utils
import os
import tempfile
import logging
import warnings

import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon
from gis_tool.spatial_utils import ensure_projected_crs, buffer_intersects_gas_lines
from gis_tool import config
from gis_tool.buffer_processor import (
    create_buffer_with_geopandas,
    merge_buffers_into_planning_file,
)

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs


def test_ensure_projected_crs_already_projected():
    """
    Test that a projected CRS is returned unchanged by ensure_projected_crs.
    """
    logger.info("Running test_ensure_projected_crs_already_projected")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=config.DEFAULT_CRS)
    projected = ensure_projected_crs(gdf)
    logger.debug(f"Original CRS: {gdf.crs}, Projected CRS: {projected.crs}")
    assert projected.crs.to_string() == config.DEFAULT_CRS
    assert projected.crs.is_projected
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
        gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=None)
        input_path = os.path.join(tmpdir, "no_crs_input.shp")
        gdf.to_file(input_path)
        logger.debug(f"Created shapefile with missing CRS at {input_path}")

        buffered_gdf = create_buffer_with_geopandas(
            input_path,
            buffer_distance_ft=config.DEFAULT_BUFFER_DISTANCE_FT
        )

        logger.debug(f"Buffered GeoDataFrame CRS: {buffered_gdf.crs}")
        assert buffered_gdf.crs is not None
        assert buffered_gdf.crs.is_projected
        assert not buffered_gdf.empty
        assert all(buffered_gdf.geometry.is_valid)
    logger.info("test_create_buffer_with_missing_crs passed.")


def test_merge_missing_crs_inputs(tmp_path):
    """
    Test merge behavior when one or both files lack a CRS.
    """
    logger.info("Running test_merge_missing_crs_inputs")
    buffer = gpd.GeoDataFrame(geometry=[Point(5, 5).buffer(5)], crs=None)
    future_dev = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:4326")

    buffer_fp, future_fp = _save_test_shapefiles(buffer, future_dev, tmp_path)

    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)
    result = gpd.read_file(future_fp)

    logger.debug(f"Merged shapefile CRS: {result.crs}, feature count: {len(result)}")
    assert not result.empty
    assert result.crs is not None
    assert all(result.geometry.is_valid)
    logger.info("test_merge_missing_crs_inputs passed.")


# NEW TESTS for CRS behavior and related functionality

def test_ensure_projected_crs_output_geometry_validity():
    """
    Test that ensure_projected_crs does not corrupt geometry validity.
    """
    logger.info("Running test_ensure_projected_crs_output_geometry_validity")
    gdf = gpd.GeoDataFrame(geometry=[Point(10, 10)], crs=config.GEOGRAPHIC_CRS)
    projected_gdf = ensure_projected_crs(gdf)
    assert all(projected_gdf.geometry.is_valid)
    logger.debug("Projected geometries are valid.")
    logger.info("test_ensure_projected_crs_output_geometry_validity passed.")


def test_create_buffer_with_geopandas_respects_default_crs(tmp_path):
    """
    Ensure create_buffer_with_geopandas assigns default CRS if missing, using config.DEFAULT_CRS.
    """
    logger.info("Running test_create_buffer_with_geopandas_respects_default_crs")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=None)
    input_path = tmp_path / "missing_crs.shp"
    gdf.to_file(str(input_path))
    result = create_buffer_with_geopandas(str(input_path), buffer_distance_ft=10.0)
    logger.debug(f"Result CRS: {result.crs}")
    assert result.crs.to_string() == config.DEFAULT_CRS
    logger.info("test_create_buffer_with_geopandas_respects_default_crs passed.")


def test_merge_buffers_into_planning_file_maintains_crs_consistency(tmp_path):
    """
      Test that the merged output has a projected CRS (as per ensure_projected_crs logic).
      """
    logger.info("Running test_merge_buffers_into_planning_file_maintains_crs_consistency")

    # Buffer layer has projected CRS (EPSG:3857)
    buffer_gdf = gpd.GeoDataFrame(
        geometry=[Point(1, 1).buffer(1)],
        crs="EPSG:3857"
    )
    # Future development layer has geographic CRS (EPSG:4326)
    future_dev_gdf = gpd.GeoDataFrame(
        geometry=[Point(2, 2)],
        crs="EPSG:4326"
    )

    buffer_fp, future_fp = _save_test_shapefiles(buffer_gdf, future_dev_gdf, tmp_path)

    merged = merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)

    result = gpd.read_file(future_fp)

    logger.debug(f"Merged file CRS: {result.crs}")

    # The final CRS should be projected (based on your ensure_projected_crs logic)
    assert result.crs.is_projected

    # Also ensure merged data contains features from both input layers
    assert len(result) >= len(buffer_gdf) + len(future_dev_gdf)
    logger.info("test_merge_buffers_into_planning_file_maintains_crs_consistency passed.")


def test_merge_buffers_into_planning_file_handles_empty_crs(tmp_path):
    """
      Test that merge_buffers_into_planning_file assigns default CRS when inputs lack CRS,
      and merges successfully.
      """
    logger.info("Running test_merge_buffers_into_planning_file_handles_empty_crs")

    # Both inputs lack CRS (crs=None)
    buffer_gdf = gpd.GeoDataFrame(geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])], crs=None)
    future_dev_gdf = gpd.GeoDataFrame(geometry=[Point(2, 2)], crs=None)

    buffer_fp, future_fp = _save_test_shapefiles(buffer_gdf, future_dev_gdf, tmp_path)

    # We expect warnings for assigning default CRS
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        merged = merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)

        # Check that warnings about default CRS assignment were triggered
        warning_msgs = [str(warning.message) for warning in w]
        assert any("Future Development layer missing CRS" in msg for msg in warning_msgs)
        assert any("Buffer layer missing CRS" in msg for msg in warning_msgs)

    # Result must have CRS assigned and be projected (buffer default is projected EPSG:32610)
    assert merged.crs is not None
    assert merged.crs.is_projected or merged.crs.to_string() == 'EPSG:4326'  # merged CRS is future dev CRS which is EPSG:4326 by default

    # File was overwritten, read back shapefile to confirm
    result = gpd.read_file(future_fp)
    assert result.crs == merged.crs
    assert len(result) == len(merged)

    logger.info("test_merge_buffers_into_planning_file_handles_empty_crs passed.")


def _save_test_shapefiles(buffer_gdf, future_dev_gdf, tmp_path):
    """
    Helper to save buffer and future development GeoDataFrames as shapefiles.
    Returns the file paths.
    """
    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"
    buffer_gdf.to_file(str(buffer_fp))
    future_dev_gdf.to_file(str(future_fp))
    logger.debug(f"Saved buffer shapefile at {buffer_fp}")
    logger.debug(f"Saved future development shapefile at {future_fp}")
    return buffer_fp, future_fp


def test_buffer_intersects_gas_lines_intersection():
    # Create a gas lines GeoDataFrame with two line geometries
    gas_lines = gpd.GeoDataFrame({
        'geometry': [
            LineString([(0, 0), (10, 0)]),
            LineString([(20, 20), (30, 20)])
        ]
    }, crs="EPSG:4326")

    # Create a buffer polygon that intersects first line
    buffer_geom = Polygon([(1, -1), (1, 1), (9, 1), (9, -1), (1, -1)])

    result = buffer_intersects_gas_lines(buffer_geom, gas_lines)
    assert result    # expect True


def test_buffer_intersects_gas_lines_no_intersection():
    gas_lines = gpd.GeoDataFrame({
        'geometry': [
            LineString([(0, 0), (10, 0)]),
            LineString([(20, 20), (30, 20)])
        ]
    }, crs="EPSG:4326")

    # Buffer polygon far away from any gas line
    buffer_geom = Polygon([(100, 100), (100, 110), (110, 110), (110, 100), (100, 100)])

    result = buffer_intersects_gas_lines(buffer_geom, gas_lines)
    assert not result  # expect False


def test_buffer_intersects_gas_lines_empty_geometry():
    gas_lines = gpd.GeoDataFrame({
        'geometry': [
            LineString([(0, 0), (10, 0)])
        ]
    }, crs="EPSG:4326")

    # Empty geometry as buffer
    buffer_geom = Polygon()

    result = buffer_intersects_gas_lines(buffer_geom, gas_lines)
    assert not result   # expect False


def test_buffer_intersects_gas_lines_with_empty_geom_in_gdf():
    gas_lines = gpd.GeoDataFrame({
        'geometry': [
            LineString([(0, 0), (10, 0)]),
            None
        ]
    }, crs="EPSG:4326")

    buffer_geom = Polygon([(1, -1), (1, 1), (9, 1), (9, -1), (1, -1)])

    result = buffer_intersects_gas_lines(buffer_geom, gas_lines)
    assert result    # expect True
