
import logging
from unittest.mock import patch

import pytest
import geopandas as gpd
from shapely.geometry import Polygon

import gis_tool.buffer_creation
from gis_tool.spatial_utils import assert_geodataframes_equal

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs

# Shared test geometry
SQUARE_POLY = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


def test_create_buffer_with_geopandas_basic(sample_gas_lines_gdf):
    # Patch gpd.read_file to return our fixture GeoDataFrame
    with patch('gis_tool.buffer_processor.gpd.read_file', return_value=sample_gas_lines_gdf):
        result_gdf = gis_tool.buffer_creation.create_buffer_with_geopandas(
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

        result_gdf = gis_tool.buffer_creation.create_buffer_with_geopandas(
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
            gis_tool.buffer_creation.create_buffer_with_geopandas(
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
    result_serial = gis_tool.buffer_creation.create_buffer_with_geopandas(
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
    result_parallel = gis_tool.buffer_creation.create_buffer_with_geopandas(
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

