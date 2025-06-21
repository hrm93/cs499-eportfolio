import os
import tempfile
import logging
import warnings

import pytest
import geopandas as gpd
from shapely.geometry import Point, Polygon

from tests.test_utils import assert_geodataframes_equal
import gis_tool.parks_subtraction

logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)  # Capture detailed logs for debugging during tests

# Shared test geometry - simple square polygon for buffer tests
SQUARE_POLY = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


def calculate_projected_area(gdf, crs="EPSG:32633"):
    """
    Helper function to calculate area of GeoDataFrame geometries after projecting to a metric CRS.

    Args:
        gdf (GeoDataFrame): Input GeoDataFrame.
        crs (str): Projected coordinate reference system (default is UTM zone 33N).

    Returns:
        pd.Series: Series of projected areas for each geometry.
    """
    projected = gdf.to_crs(crs)
    return projected.geometry.area


def test_subtract_parks_from_buffer_behavior():
    """
    Test that subtract_parks_from_buffer correctly subtracts multiple park polygons
    from a buffer GeoDataFrame, resulting in smaller buffered areas.
    """
    logger.info("Running test_subtract_parks_from_buffer_behavior")

    # Create a buffer GeoDataFrame around point (0,0)
    buffer_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(10)],
        crs="EPSG:4326"
    )
    # Create parks GeoDataFrame with one overlapping and one distant polygon
    parks_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(5), Point(20, 20).buffer(3)],
        crs="EPSG:4326"
    )

    # Write parks data to a temporary GeoJSON file for the function input
    with tempfile.TemporaryDirectory() as tmpdir:
        parks_path = os.path.join(tmpdir, "parks.geojson")
        parks_gdf.to_file(parks_path, driver="GeoJSON")

        # Perform subtraction
        result_gdf = gis_tool.parks_subtraction.subtract_parks_from_buffer(buffer_gdf, parks_path)

        # Validate output type and row count consistency
        assert isinstance(result_gdf, gpd.GeoDataFrame)
        assert len(result_gdf) == len(buffer_gdf)

        # Area after subtraction should be smaller than original buffer area
        original_area = calculate_projected_area(buffer_gdf).iloc[0]
        result_area = calculate_projected_area(result_gdf).iloc[0]
        assert result_area < original_area, "Park subtraction did not reduce buffer area."

    logger.info("test_subtract_parks_from_buffer_behavior passed.")


def test_subtract_parks_from_buffer_buffer_fully_within_park():
    """
    Test when the buffer geometry is fully enclosed by a park polygon,
    the result should be empty or have zero area after subtraction.
    """
    logger.info("Running test_subtract_parks_from_buffer_buffer_fully_within_park")

    buffer_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(5)],
        crs="EPSG:4326"
    )
    parks_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(10)],  # Larger park fully covering buffer
        crs="EPSG:4326"
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        parks_path = os.path.join(tmpdir, "parks.geojson")
        parks_gdf.to_file(parks_path, driver="GeoJSON")

        result_gdf = gis_tool.parks_subtraction.subtract_parks_from_buffer(buffer_gdf, parks_path)

        # Either GeoDataFrame is empty (all geometry removed) or area is zero
        if result_gdf.empty:
            assert True  # Expected empty result
        else:
            result_area = calculate_projected_area(result_gdf).iloc[0]
            assert result_area == 0, "Expected zero area when buffer fully subtracted."

    logger.info("test_subtract_parks_from_buffer_buffer_fully_within_park passed.")


def test_subtract_parks_from_buffer_no_parks(sample_gas_lines_gdf):
    """
    Test subtract_parks_from_buffer with parks_path=None returns input buffer unchanged.

    This verifies the function behaves as a no-op if no parks file is provided.
    """
    logger.info("Running test_subtract_parks_from_buffer_no_parks")

    buffered_gdf = sample_gas_lines_gdf.copy()
    buffered_gdf['geometry'] = buffered_gdf.geometry.buffer(10)

    result_gdf = gis_tool.parks_subtraction.subtract_parks_from_buffer(buffered_gdf, parks_path=None)

    # Use utility to assert deep equality of GeoDataFrames (geometry and attributes)
    assert_geodataframes_equal(buffered_gdf, result_gdf)
    assert result_gdf.equals(buffered_gdf), "Result should be identical when no parks file provided."

    logger.info("test_subtract_parks_from_buffer_no_parks passed.")


def test_subtract_parks_from_buffer_invalid_input():
    """
    Test subtract_parks_from_buffer raises an Exception if parks_path is invalid.

    Expects a clear error message referencing the missing file.
    """
    logger.info("Running test_subtract_parks_from_buffer_invalid_input")

    buffer_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0).buffer(10)],
        crs="EPSG:4326"
    )

    # Capture exception and verify message contains missing file name
    with pytest.raises(Exception, match=".*non_existent_file.geojson.*"):
        gis_tool.parks_subtraction.subtract_parks_from_buffer(buffer_gdf, parks_path="non_existent_file.geojson")

    logger.info("test_subtract_parks_from_buffer_invalid_input passed.")


def create_buffer_polygons():
    """
    Helper function to create a GeoDataFrame containing a simple square polygon buffer.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with one polygon geometry.
    """
    return gpd.GeoDataFrame({'id': [1], 'geometry': [SQUARE_POLY]}, crs="EPSG:4326")


def test_subtract_parks_from_buffer_multiprocessing(sample_gas_lines_gdf, sample_parks_file):
    """
    Test subtract_parks_from_buffer with multiprocessing enabled and disabled,
    ensuring output GeoDataFrames are identical.

    This test confirms multiprocessing does not alter output correctness.

    Args:
        sample_gas_lines_gdf (GeoDataFrame): Sample gas lines geometries.
        sample_parks_file (str): Path to parks GeoJSON file.
    """
    logger.info("Starting test: subtract_parks_from_buffer with and without multiprocessing.")

    buffer_distance = 100  # Buffer distance in meters
    buffered_gdf = sample_gas_lines_gdf.copy()
    buffered_gdf['geometry'] = buffered_gdf.geometry.buffer(buffer_distance)
    logger.debug("Buffered gas lines geometries created.")

    # Run without multiprocessing
    logger.info("Testing subtract_parks_from_buffer without multiprocessing.")
    result_serial = gis_tool.parks_subtraction.subtract_parks_from_buffer(
        buffered_gdf.copy(), sample_parks_file, use_multiprocessing=False
    )
    logger.debug("Serial subtraction completed.")

    # Run with multiprocessing enabled
    logger.info("Testing subtract_parks_from_buffer with multiprocessing.")
    result_parallel = gis_tool.parks_subtraction.subtract_parks_from_buffer(
        buffered_gdf.copy(), sample_parks_file, use_multiprocessing=True
    )
    logger.debug("Parallel subtraction completed.")

    # Verify that outputs are equal in geometry and data
    logger.info("Asserting that serial and parallel results are identical.")
    assert_geodataframes_equal(result_serial, result_parallel)
    logger.info("Test passed: Outputs are consistent across multiprocessing modes.")
