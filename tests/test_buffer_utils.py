### Tests for buffer_utils

import logging
import pytest
import geopandas as gpd

from shapely.geometry import Point, Polygon

import gis_tool.buffer_creation
from gis_tool import buffer_processor
from gis_tool.buffer_utils import (
    buffer_geometry,
    buffer_geometry_helper,
    subtract_park_from_geom,
    subtract_park_from_geom_helper,
)

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


def test_buffer_geometry_simple():
    """
    Test buffering a simple point geometry using buffer_geometry.
    """
    logger.info("Running test_buffer_geometry_simple")
    point = Point(1, 1)
    buffer_dist = 10
    buffered = buffer_geometry(point, buffer_dist)
    assert buffered.area > 0
    assert buffered.contains(point)
    logger.info("test_buffer_geometry_simple passed.")


def test_buffer_geometry_helper_consistency():
    """
    Test buffer_geometry_helper returns consistent results with buffer_geometry.
    Assumes buffer_geometry_helper uses a fixed buffer distance internally.
    """
    logger.info("Running test_buffer_geometry_helper_consistency")
    geom = Point(0, 0)
    fixed_dist = 10
    result_main = buffer_geometry(geom, fixed_dist)
    result_helper = buffer_geometry_helper((geom, fixed_dist))  # pass as tuple
    assert result_main.equals(result_helper)
    logger.info("test_buffer_geometry_helper_consistency passed.")


def test_subtract_park_from_geom_difference():
    """
    Test that subtract_park_from_geom subtracts the park geometry from a buffer geometry.
    """
    logger.info("Running test_subtract_park_from_geom_difference")
    buffer_geom = Point(0, 0).buffer(10)
    park_geoms = [Point(0, 0).buffer(5)]  # Make it a list
    result = subtract_park_from_geom(buffer_geom, park_geoms)
    assert result.area < buffer_geom.area
    logger.info("test_subtract_park_from_geom_difference passed.")


def test_subtract_park_from_geom_helper_equivalence():
    """
    Test subtract_park_from_geom_helper provides the same result as subtract_park_from_geom
    when given the same input geometries.
    """
    logger.info("Running test_subtract_park_from_geom_helper_equivalence")

    buffer_geom = Point(0, 0).buffer(8)
    parks_geoms = [Point(1, 1).buffer(3)]  # can be a list or any collection

    result_main = subtract_park_from_geom(buffer_geom, parks_geoms)
    result_helper = subtract_park_from_geom_helper((buffer_geom, parks_geoms))

    assert result_main.equals(result_helper)
    logger.info("test_subtract_park_from_geom_helper_equivalence passed.")

    # If possible to access helper's internal park geometry or patch it,
    # then compare results more strictly.
    logger.info("test_subtract_park_from_geom_helper_equivalence passed.")


def test_merge_future_dev_mixed_geometry(tmp_path):
    """
    Test merging when future development file has mixed geometry types.
    """
    logger.info("Starting test_merge_future_dev_mixed_geometry")
    future_dev = gpd.GeoDataFrame(
        geometry=[
            Point(0, 0),
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        ],
        crs="EPSG:32633"
    )
    buffer = gpd.GeoDataFrame(
        geometry=[Polygon([(2, 2), (3, 2), (3, 3), (2, 3)])],
        crs="EPSG:32633"
    )

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "mixed_future.geojson"

    buffer.to_file(str(buffer_fp))
    future_dev.to_file(str(future_fp), driver="GeoJSON")
    logger.debug(f"Created buffer shapefile at {buffer_fp} and future GeoJSON at {future_fp} with mixed geometries")

    with pytest.raises(ValueError, match=r"mixed geometry types"):
        logger.info("Calling merge_buffers_into_planning_file expecting ValueError due to mixed geometry types")
        buffer_processor.merge_buffers_into_planning_file(
            str(buffer_fp),
            str(future_fp),
            point_buffer_distance=10.0
        )
    logger.info("Completed test_merge_future_dev_mixed_geometry")


def test_final_geometry_validation_removes_invalid_and_empty(tmp_path, sample_gas_lines_gdf):
    """
    Test that create_buffer_with_geopandas removes invalid and empty geometries in the final output.
    """
    # Create an invalid geometry intentionally
    invalid_geom = Polygon([(0, 0), (1, 1), (1, 0), (0, 0)])  # self-intersecting polygon
    empty_geom = Polygon()  # Empty geometry

    sample_gas_lines_gdf.loc[len(sample_gas_lines_gdf)] = [invalid_geom]
    sample_gas_lines_gdf.loc[len(sample_gas_lines_gdf)] = [empty_geom]

    gas_lines_path = tmp_path / "gas_lines.geojson"
    sample_gas_lines_gdf.to_file(str(gas_lines_path), driver='GeoJSON')

    result = gis_tool.buffer_creation.create_buffer_with_geopandas(
        input_gas_lines_path=str(gas_lines_path),
        buffer_distance_ft=328.084,
        parks_path=None,
        use_multiprocessing=False,
    )

    # Assert no invalid or empty geometries remain
    assert all(result.geometry.is_valid), "Final output still contains invalid geometries"
    assert not any(result.geometry.is_empty), "Final output still contains empty geometries"
