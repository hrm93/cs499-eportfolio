# test buffer_processor:
import os
import tempfile
import logging
from unittest.mock import patch

import pytest
import geopandas as gpd
from shapely.geometry import Point, Polygon

from gis_tool.spatial_utils import assert_geodataframes_equal
from gis_tool import buffer_processor
from gis_tool.buffer_processor import (
    merge_buffers_into_planning_file,
    subtract_parks_from_buffer,
)

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


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
        parks_gdf.to_file(parks_path, driver="GeoJSON")  # Save parks to GeoJSON file

        result_gdf = subtract_parks_from_buffer(buffer_gdf, parks_path)
        # Add assertions about result_gdf here

    logger.info("test_subtract_parks_from_buffer_behavior passed.")


def test_create_buffer_with_geopandas_basic(sample_gas_lines_gdf):
    # Patch gpd.read_file to return our fixture GeoDataFrame
    with patch('gis_tool.buffer_processor.gpd.read_file', return_value=sample_gas_lines_gdf):
        result_gdf = buffer_processor.create_buffer_with_geopandas(
            input_gas_lines_path="fake_path.shp",
            buffer_distance_ft=10,
            parks_path=None,
            use_multiprocessing=False
        )

        # Assert result is GeoDataFrame
        assert isinstance(result_gdf, gpd.GeoDataFrame)

        # Buffer distance in meters ~ 3.048m (10 ft)
        # The buffered geometries should still intersect original lines
        # So result_gdf should not be empty and all geometries valid
        assert not result_gdf.empty
        assert result_gdf.geometry.is_valid.all()
        assert (~result_gdf.geometry.is_empty).all()


def test_create_buffer_with_geopandas_with_parks(sample_gas_lines_gdf):
    # Create parks GeoDataFrame with one polygon overlapping buffer area
    parks_gdf = gpd.GeoDataFrame({
        'geometry': [
            Polygon([(3, -2), (3, 8), (8, 8), (8, -2), (3, -2)])
        ]
    }, crs="EPSG:4326")

    # Patch both gas lines and parks reading
    with patch('gis_tool.buffer_processor.gpd.read_file') as mock_read_file:
        # The first call returns gas_lines, second returns parks
        mock_read_file.side_effect = [sample_gas_lines_gdf, parks_gdf]

        result_gdf = buffer_processor.create_buffer_with_geopandas(
            input_gas_lines_path="fake_gas.shp",
            buffer_distance_ft=10,
            parks_path="fake_parks.shp",
            use_multiprocessing=False
        )

    # The parks area should be subtracted from the buffer
    # Result geometries should be valid and not empty
    assert not result_gdf.empty
    assert result_gdf.geometry.is_valid.all()
    assert (~result_gdf.geometry.is_empty).all()


def test_create_buffer_with_geopandas_invalid_input():
    with patch('gis_tool.buffer_processor.gpd.read_file', side_effect=Exception("File not found")):
        with pytest.raises(Exception):
            buffer_processor.create_buffer_with_geopandas(
                input_gas_lines_path="invalid_path.shp",
                buffer_distance_ft=10,
                parks_path=None,
                use_multiprocessing=False
            )


def test_create_buffer_with_geopandas_multiprocessing(tmp_path, sample_gas_lines_gdf, sample_parks_file):
    """
    Test the `create_buffer_with_geopandas` function with and without multiprocessing
    to verify that both modes produce identical GeoDataFrame outputs.

    This test writes the sample gas lines GeoDataFrame to a temporary GeoJSON file,
    then calls the buffering function twice: once without multiprocessing and once with.
    It asserts that the resulting GeoDataFrames are equal to ensure consistent behavior.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Temporary directory path provided by pytest for storing intermediate files.
    sample_gas_lines_gdf :
        GeoDataFrame containing sample gas line geometries.
    sample_parks_file : str or Path
        File path to the parks data used for subtracting from buffers.
    """
    logger.info("Starting test: create_buffer_with_geopandas_multiprocessing to compare serial and parallel outputs.")

    gas_lines_path = tmp_path / "gas_lines.geojson"
    sample_gas_lines_gdf.to_file(str(gas_lines_path), driver='GeoJSON')
    logger.debug(f"Sample gas lines written to GeoJSON: {gas_lines_path}")

    # Test without multiprocessing
    logger.info("Calling create_buffer_with_geopandas without multiprocessing.")
    result_serial = buffer_processor.create_buffer_with_geopandas(
        input_gas_lines_path=str(gas_lines_path),
        buffer_distance_ft=328.084,  # 100 meters in feet
        parks_path=sample_parks_file,
        use_multiprocessing=False,
    )
    logger.debug("Buffering (serial) completed.")

    assert all(result_serial.geometry.is_valid), "Serial output contains invalid geometries"
    assert not any(result_serial.geometry.is_empty), "Serial output contains empty geometries"

    # Test with multiprocessing
    logger.info("Calling create_buffer_with_geopandas with multiprocessing.")
    result_parallel = buffer_processor.create_buffer_with_geopandas(
        input_gas_lines_path=str(gas_lines_path),
        buffer_distance_ft=328.084,
        parks_path=sample_parks_file,
        use_multiprocessing=True,
    )
    logger.debug("Buffering (parallel) completed.")

    assert all(result_parallel.geometry.is_valid), "Parallel output contains invalid geometries"
    assert not any(result_parallel.geometry.is_empty), "Parallel output contains empty geometries"

    # Use helper assertion
    logger.info("Asserting equality of serial and parallel GeoDataFrame results.")

    # Assert both outputs equal
    assert_geodataframes_equal(result_serial, result_parallel)

    logger.info("Test passed: Serial and parallel outputs match.")


def test_merge_buffers_into_planning_file(tmp_path):
    """
    Test merging buffer polygons into a Future Development planning shapefile by:
    - Creating temporary buffer polygons and future development shapefiles.
    - Calling `merge_buffers_into_planning_file` with buffer and future dev paths.
    - Asserting merged GeoDataFrame contains features from both inputs.
    - Validating geometry types are polygons or multipolygons.
    """
    # Create buffer polygon GeoDataFrame (square polygon)
    logger.info("Running test_merge_buffers_into_planning_file")
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    buffer_polygons = gpd.GeoDataFrame({'id': [1], 'geometry': [poly]}, crs="EPSG:4326")

    future_points = gpd.GeoDataFrame(
        {'id': [10], 'geometry': [Point(2, 2)]},
        crs="EPSG:4326"
    )

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"
    buffer_polygons.to_file(str(buffer_fp), driver='ESRI Shapefile')
    future_points.to_file(str(future_fp), driver='ESRI Shapefile')
    logger.debug(f"Created buffer and future development shapefiles at {buffer_fp} and {future_fp}")

    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=10.0)
    merged_gdf = gpd.read_file(future_fp)

    logger.debug(f"Merged GeoDataFrame has {len(merged_gdf)} features.")
    assert len(merged_gdf) == 2
    assert all(geom in ['Polygon', 'MultiPolygon'] for geom in merged_gdf.geom_type)
    logger.info("test_merge_buffers_into_planning_file passed.")


def test_merge_empty_buffer_file(tmp_path):
    """
    Test that merge function skips writing if the buffer file is empty,
    avoiding Fiona empty write warning.
    """
    logger.info("Starting test: merge_empty_buffer_file to ensure merge skips empty buffers.")

    # Create empty buffer GeoDataFrame
    empty_buffer = gpd.GeoDataFrame(geometry=[], crs="EPSG:32633")
    logger.debug("Created empty buffer GeoDataFrame.")

    # Create future development GeoDataFrame with one point
    future_dev = gpd.GeoDataFrame(geometry=[Point(2, 2)], crs="EPSG:32633")
    logger.debug("Created future development GeoDataFrame with one point.")

    buffer_fp = tmp_path / "empty_buffer.geojson"
    future_fp = tmp_path / "future_dev.shp"

    # Write empty buffer as GeoJSON (empty file)
    empty_buffer.to_file(str(buffer_fp), driver="GeoJSON")
    logger.debug(f"Wrote empty buffer GeoJSON to: {buffer_fp}")

    # Write future development shapefile (non-empty)
    future_dev.to_file(str(future_fp))
    logger.debug(f"Wrote future development shapefile to: {future_fp}")

    # Read original future development file contents for later comparison
    original_future = gpd.read_file(future_fp)
    logger.debug("Read original future development file.")

    # Call merge; this should skip writing empty merged file and return merged GeoDataFrame
    logger.info("Calling merge_buffers_into_planning_file with empty buffer file.")
    merged_gdf = merge_buffers_into_planning_file(
        str(buffer_fp), str(future_fp), point_buffer_distance=10.0
    )
    logger.debug("Merge function called successfully.")

    # After merge, read the future file again
    after_merge_future = gpd.read_file(future_fp)
    logger.debug("Read future development file after merge.")

    # Assert merged GeoDataFrame is not empty and matches the original future dev data (unchanged)
    logger.info("Asserting that merged GeoDataFrame is not empty and future dev file is unchanged.")
    assert not merged_gdf.empty
    assert len(merged_gdf) == len(original_future)
    assert after_merge_future.equals(original_future), (
        "Future dev file should not be overwritten if buffer is empty"
    )
    logger.debug("Assertions passed for empty buffer merge behavior.")

    # Confirm merged_gdf has same CRS as future_dev
    assert merged_gdf.crs == future_dev.crs
    logger.info("Test passed: merge_empty_buffer_file behavior is as expected.")


def test_merge_buffer_non_polygon(tmp_path):
    """
    Test that a ValueError is raised if the buffer shapefile contains non-polygon geometries.
    """
    logger.info("Starting test_merge_buffer_non_polygon")
    gdf = gpd.GeoDataFrame(geometry=[Point(1, 1)], crs="EPSG:32633")
    buffer_fp = tmp_path / "nonpoly_buffer.shp"
    gdf.to_file(str(buffer_fp))
    logger.debug(f"Created buffer shapefile with non-polygon geometry at {buffer_fp}")

    future_gdf = gpd.GeoDataFrame(geometry=[Point(5, 5)], crs="EPSG:32633")
    future_fp = tmp_path / "future_dev.shp"
    future_gdf.to_file(str(future_fp))
    logger.debug(f"Created future development shapefile at {future_fp}")

    with pytest.raises(ValueError, match="polygon or multipolygon"):
        logger.info("Calling merge_buffers_into_planning_file expecting ValueError due to non-polygon geometry")
        merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)
    logger.info("Completed test_merge_buffer_non_polygon")


def test_merge_saves_to_geojson(tmp_path):
    """
    Test saving merged output to a GeoJSON file.
    """
    logger.info("Starting test_merge_saves_to_geojson")
    buffer = gpd.GeoDataFrame(geometry=[Point(1, 1).buffer(1)], crs="EPSG:4326")
    future_dev = gpd.GeoDataFrame(geometry=[Point(2, 2)], crs="EPSG:4326")

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.geojson"  # Save to GeoJSON!
    buffer.to_file(str(buffer_fp))
    future_dev.to_file(str(future_fp), driver="GeoJSON")
    logger.debug(f"Created buffer shapefile at {buffer_fp} and future GeoJSON at {future_fp}")

    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)
    logger.info("Called merge_buffers_into_planning_file to merge and save GeoJSON output")

    result = gpd.read_file(future_fp)
    logger.debug(f"Read merged GeoJSON file, features count: {len(result)}")

    assert future_fp.exists()
    assert len(result) == 2
    logger.info("Completed test_merge_saves_to_geojson")


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
    result_serial = buffer_processor.subtract_parks_from_buffer(
        buffered_gdf.copy(), sample_parks_file, use_multiprocessing=False
    )
    logger.debug("Serial subtraction completed.")

    # Test with multiprocessing
    logger.info("Testing subtract_parks_from_buffer with multiprocessing.")
    result_parallel = buffer_processor.subtract_parks_from_buffer(
        buffered_gdf.copy(), sample_parks_file, use_multiprocessing=True
    )
    logger.debug("Parallel subtraction completed.")

    # Verify that both outputs are identical
    logger.info("Asserting that serial and parallel results are identical.")
    assert_geodataframes_equal(result_serial, result_parallel)
    logger.info("Test passed: Outputs are consistent across modes.")
