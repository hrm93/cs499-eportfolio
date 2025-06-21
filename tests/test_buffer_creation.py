import logging
from unittest.mock import patch

import pytest
import geopandas as gpd
from shapely.geometry import Polygon

import gis_tool.buffer_creation
from tests.test_utils import assert_geodataframes_equal

logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs for detailed trace

# Shared test geometry: A simple square polygon used in multiple tests
SQUARE_POLY = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


def test_create_buffer_with_geopandas_basic(sample_gas_lines_gdf):
    """
    Test basic functionality of `create_buffer_with_geopandas` using a mocked input GeoDataFrame.

    This test patches GeoPandas' read_file method to return a predefined gas lines GeoDataFrame
    fixture instead of reading from disk. It then calls the buffering function with:
    - a fake input path (ignored due to patch)
    - a buffer distance of 10 feet (converted internally to meters)
    - no parks path to subtract
    - no multiprocessing

    It verifies:
    - The output is a GeoDataFrame
    - The output GeoDataFrame is not empty
    - All geometries in the output are valid and non-empty
    """
    logger.info("Running test_create_buffer_with_geopandas_basic")

    # Patch gpd.read_file inside gis_tool.buffer_processor to return fixture data
    with patch('gis_tool.buffer_processor.gpd.read_file', return_value=sample_gas_lines_gdf):
        result_gdf = gis_tool.buffer_creation.create_buffer_with_geopandas(
            input_gas_lines_path="fake_path.shp",
            buffer_distance_ft=10,
            parks_path=None,
            use_multiprocessing=False
        )

        # Verify output is a GeoDataFrame instance
        assert isinstance(result_gdf, gpd.GeoDataFrame)

        # Check that output is not empty, geometries are valid, and not empty
        assert not result_gdf.empty
        assert result_gdf.geometry.is_valid.all()
        assert (~result_gdf.geometry.is_empty).all()


def test_create_buffer_with_geopandas_with_parks(sample_gas_lines_gdf):
    """
    Test buffering function when subtracting park areas.

    This test creates a sample parks GeoDataFrame with one polygon overlapping
    the buffered gas lines area. The test patches the read_file function twice:
    first returning gas lines, then parks GeoDataFrame.

    It verifies that the buffered gas lines after subtracting parks:
    - Are valid geometries
    - Are not empty
    - The function completes successfully without errors
    """
    logger.info("Running test_create_buffer_with_geopandas_with_parks")

    # Create a parks GeoDataFrame polygon overlapping buffered gas lines
    parks_gdf = gpd.GeoDataFrame({
        'geometry': [
            Polygon([(3, -2), (3, 8), (8, 8), (8, -2), (3, -2)])
        ]
    }, crs="EPSG:4326")

    # Patch read_file to first return gas lines, then parks GeoDataFrame
    with patch('gis_tool.buffer_processor.gpd.read_file') as mock_read_file:
        mock_read_file.side_effect = [sample_gas_lines_gdf, parks_gdf]

        result_gdf = gis_tool.buffer_creation.create_buffer_with_geopandas(
            input_gas_lines_path="fake_gas.shp",
            buffer_distance_ft=10,
            parks_path="fake_parks.shp",
            use_multiprocessing=False
        )

    # Assert results are valid and not empty after parks subtraction
    assert not result_gdf.empty
    assert result_gdf.geometry.is_valid.all()
    assert (~result_gdf.geometry.is_empty).all()


def test_create_buffer_with_geopandas_invalid_input():
    """
    Test that the buffering function raises an exception when input file reading fails.

    This test patches GeoPandas' read_file to raise an Exception, simulating
    a file-not-found or read error scenario. It verifies that the buffering
    function propagates the exception as expected.
    """
    logger.info("Running test_create_buffer_with_geopandas_invalid_input")

    with patch('gis_tool.buffer_processor.gpd.read_file', side_effect=Exception("File not found")):
        with pytest.raises(Exception):
            gis_tool.buffer_creation.create_buffer_with_geopandas(
                input_gas_lines_path="invalid_path.shp",
                buffer_distance_ft=10,
                parks_path=None,
                use_multiprocessing=False
            )


def test_create_buffer_with_geopandas_multiprocessing(tmp_path, sample_gas_lines_gdf, sample_parks_file):
    """
    Test `create_buffer_with_geopandas` with multiprocessing enabled and disabled.

    This test writes the sample gas lines GeoDataFrame to a temporary GeoJSON file,
    then calls the buffering function twice:
    - Once without multiprocessing (serial)
    - Once with multiprocessing (parallel)

    It verifies that:
    - Both outputs contain valid, non-empty geometries
    - The serial and parallel outputs are geometrically equivalent using a helper assertion

    Parameters
    ----------
    tmp_path : pathlib.Path
        Temporary directory for writing intermediate files.
    sample_gas_lines_gdf : geopandas.GeoDataFrame
        Sample gas lines for buffering.
    sample_parks_file : str or pathlib.Path
        File path to the parks shapefile/GeoJSON used for subtracting buffer areas.
    """
    logger.info("Starting test: create_buffer_with_geopandas_multiprocessing to compare serial and parallel outputs.")

    # Write sample gas lines to a temporary GeoJSON file for testing
    gas_lines_path = tmp_path / "gas_lines.geojson"
    sample_gas_lines_gdf.to_file(str(gas_lines_path), driver='GeoJSON')
    logger.debug(f"Sample gas lines written to GeoJSON: {gas_lines_path}")

    # Call buffering function without multiprocessing (serial)
    logger.info("Calling create_buffer_with_geopandas without multiprocessing.")
    result_serial = gis_tool.buffer_creation.create_buffer_with_geopandas(
        input_gas_lines_path=str(gas_lines_path),
        buffer_distance_ft=328.084,  # 100 meters converted to feet
        parks_path=sample_parks_file,
        use_multiprocessing=False,
    )
    logger.debug("Buffering (serial) completed.")

    # Check serial output geometries are valid and non-empty
    assert all(result_serial.geometry.is_valid), "Serial output contains invalid geometries"
    assert not any(result_serial.geometry.is_empty), "Serial output contains empty geometries"

    # Call buffering function with multiprocessing enabled (parallel)
    logger.info("Calling create_buffer_with_geopandas with multiprocessing.")
    result_parallel = gis_tool.buffer_creation.create_buffer_with_geopandas(
        input_gas_lines_path=str(gas_lines_path),
        buffer_distance_ft=328.084,
        parks_path=sample_parks_file,
        use_multiprocessing=True,
    )
    logger.debug("Buffering (parallel) completed.")

    # Check parallel output geometries are valid and non-empty
    assert all(result_parallel.geometry.is_valid), "Parallel output contains invalid geometries"
    assert not any(result_parallel.geometry.is_empty), "Parallel output contains empty geometries"

    # Assert both serial and parallel outputs are equivalent using custom helper
    logger.info("Asserting equality of serial and parallel GeoDataFrame results.")
    assert_geodataframes_equal(result_serial, result_parallel)

    logger.info("Test passed: Serial and parallel outputs match.")
