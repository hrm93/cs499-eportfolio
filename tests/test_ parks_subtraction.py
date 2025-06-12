
import os
import tempfile
import logging

import pytest
import geopandas as gpd
from shapely.geometry import Point, Polygon

from gis_tool.spatial_utils import assert_geodataframes_equal
import gis_tool.parks_subtraction

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs

# Shared test geometry
SQUARE_POLY = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


def calculate_projected_area(gdf, crs="EPSG:32633"):
    projected = gdf.to_crs(crs)
    return projected.geometry.area


def test_subtract_parks_from_buffer_behavior():
    """
    Test subtract_parks_from_buffer subtracts multiple park polygons from a buffer GeoDataFrame.
    """
    logger.info("Running test_subtract_parks_from_buffer_behavior")
    buffer_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(10)],
        crs="EPSG:4326"
    )
    parks_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(5), Point(20, 20).buffer(3)],
        crs="EPSG:4326"
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        parks_path = os.path.join(tmpdir, "parks.geojson")
        parks_gdf.to_file(parks_path, driver="GeoJSON")

        # Subtract parks
        result_gdf = gis_tool.parks_subtraction.subtract_parks_from_buffer(buffer_gdf, parks_path)

        # Valid GeoDataFrame, same number of rows
        assert isinstance(result_gdf, gpd.GeoDataFrame)
        assert len(result_gdf) == len(buffer_gdf)

        # Area comparison: result area must be smaller
        original_area = calculate_projected_area(buffer_gdf).iloc[0]
        result_area = calculate_projected_area(result_gdf).iloc[0]
        assert result_area < original_area, "Park subtraction did not reduce buffer area."

    logger.info("test_subtract_parks_from_buffer_behavior passed.")


def test_subtract_parks_from_buffer_buffer_fully_within_park():
    """
    Test case where buffer is fully inside a park polygon.
    Resulting geometry should be empty or None after subtraction.
    """
    logger.info("Running test_subtract_parks_from_buffer_buffer_fully_within_park")
    buffer_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(5)],
        crs="EPSG:4326"
    )
    parks_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(10)],  # bigger park covering buffer entirely
        crs="EPSG:4326"
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        parks_path = os.path.join(tmpdir, "parks.geojson")
        parks_gdf.to_file(parks_path, driver="GeoJSON")

        result_gdf = gis_tool.parks_subtraction.subtract_parks_from_buffer(buffer_gdf, parks_path)

        if result_gdf.empty:
            # Expected: result_gdf is completely empty
            assert True
        else:
            # If not empty, verify area is zero
            result_area = calculate_projected_area(result_gdf).iloc[0]
            assert result_area == 0, "Expected area to be zero when buffer is fully subtracted."

    logger.info("test_subtract_parks_from_buffer_buffer_fully_within_park passed.")


def test_subtract_parks_from_buffer_no_parks(sample_gas_lines_gdf):
    """
    Test subtract_parks_from_buffer with parks_path=None returns input buffer unchanged.
    """
    logger.info("Running test_subtract_parks_from_buffer_no_parks")
    buffered_gdf = sample_gas_lines_gdf.copy()
    buffered_gdf['geometry'] = buffered_gdf.geometry.buffer(10)

    result_gdf = gis_tool.parks_subtraction.subtract_parks_from_buffer(buffered_gdf, parks_path=None)

    # Validate equality
    assert_geodataframes_equal(buffered_gdf, result_gdf)
    assert result_gdf.equals(buffered_gdf), "Result should be identical when no parks file."

    logger.info("test_subtract_parks_from_buffer_no_parks passed.")


def test_subtract_parks_from_buffer_invalid_input():
    """
    Test subtract_parks_from_buffer with invalid parks file path raises Exception.
    """
    logger.info("Running test_subtract_parks_from_buffer_invalid_input")
    buffer_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(10)],
        crs="EPSG:4326"
    )
    with pytest.raises(Exception, match=".*non_existent_file.geojson.*"):
        gis_tool.parks_subtraction.subtract_parks_from_buffer(buffer_gdf, parks_path="non_existent_file.geojson")

    logger.info("test_subtract_parks_from_buffer_invalid_input passed.")


# Helper to create the shared buffer polygons GeoDataFrame
def create_buffer_polygons():
    return gpd.GeoDataFrame({'id': [1], 'geometry': [SQUARE_POLY]}, crs="EPSG:4326")


def test_subtract_parks_from_buffer_multiprocessing(sample_gas_lines_gdf, sample_parks_file):
    """
    Test the `subtract_parks_from_buffer` function with and without multiprocessing
    to ensure consistent results.

    This test creates buffered geometries around sample gas lines and calls the
    `subtract_parks_from_buffer` function twice: once with multiprocessing disabled,
    and once with multiprocessing enabled. It asserts that the resulting GeoDataFrames
    are equal, verifying that multiprocessing does not affect the output correctness.

    Parameters
    ----------
    sample_gas_lines_gdf :
        GeoDataFrame containing geometries of gas lines to buffer.
    sample_parks_file :
        Path to the parks data used to subtract from the buffered geometries.
    """
    # Buffer the gas lines (non-multiprocessing)
    logger.info("Starting test: subtract_parks_from_buffer with and without multiprocessing.")

    buffer_distance = 100  # meters
    buffered_gdf = sample_gas_lines_gdf.copy()
    buffered_gdf['geometry'] = buffered_gdf.geometry.buffer(buffer_distance)
    logger.debug("Buffered gas lines geometries created.")

    # Test without multiprocessing
    logger.info("Testing subtract_parks_from_buffer without multiprocessing.")
    result_serial = gis_tool.parks_subtraction.subtract_parks_from_buffer(
        buffered_gdf.copy(), sample_parks_file, use_multiprocessing=False
    )
    logger.debug("Serial subtraction completed.")

    # Test with multiprocessing
    logger.info("Testing subtract_parks_from_buffer with multiprocessing.")
    result_parallel = gis_tool.parks_subtraction.subtract_parks_from_buffer(
        buffered_gdf.copy(), sample_parks_file, use_multiprocessing=True
    )
    logger.debug("Parallel subtraction completed.")

    # Verify that both outputs are identical
    logger.info("Asserting that serial and parallel results are identical.")
    assert_geodataframes_equal(result_serial, result_parallel)
    logger.info("Test passed: Outputs are consistent across modes.")

