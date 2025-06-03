# test buffer_processor:
import os
import tempfile
import logging

import pytest
import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon

from gis_tool import buffer_processor, config
from gis_tool.buffer_processor import (
    create_buffer_with_geopandas,
    merge_buffers_into_planning_file,
    buffer_geometry,
    buffer_geometry_helper,
    subtract_park_from_geom,
    subtract_park_from_geom_helper,
    parallel_process,
    subtract_parks_from_buffer,
)

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


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


def assert_geodataframes_equal(gdf1, gdf2, tol=1e-6):
    """
      Assert that two GeoDataFrames are equal in terms of CRS, length, and geometry,
      with geometries compared using an exact match within a given tolerance.

      Parameters
      ----------
      gdf1 : geopandas.GeoDataFrame
          The first GeoDataFrame to compare.
      gdf2 : geopandas.GeoDataFrame
          The second GeoDataFrame to compare.
      tol : float, optional
          Tolerance for geometry equality comparison (default is 1e-6).

      Raises
      ------
      AssertionError
          If any of the checks (type, CRS, length, geometry equality) fail.
      """
    logger.info("Comparing two GeoDataFrames for equality.")
    assert isinstance(gdf1, gpd.GeoDataFrame), "First input is not a GeoDataFrame"
    assert isinstance(gdf2, gpd.GeoDataFrame), "Second input is not a GeoDataFrame"
    logger.debug(f"CRS check: {gdf1.crs} == {gdf2.crs}")
    assert gdf1.crs == gdf2.crs, "CRS mismatch"
    logger.debug(f"Length check: {len(gdf1)} == {len(gdf2)}")
    assert len(gdf1) == len(gdf2), "GeoDataFrames have different lengths"

    for i, (geom1, geom2) in enumerate(zip(gdf1.geometry, gdf2.geometry)):
        equal = geom1.equals_exact(geom2, tolerance=tol)
        logger.debug(f"Geometry check at index {i}: {'equal' if equal else 'not equal'}")
        assert equal, f"Geometries at index {i} differ"

    logger.info("GeoDataFrames are equal.")


def test_geometry_simplification_accuracy():
    """
    Test that simplifying buffered geometries preserves topology and does not lose data integrity.
    """
    poly = Point(0, 0).buffer(10)  # Big buffer
    gdf = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:4326")
    simplified = gdf.geometry.simplify(1.0)  # Simplify with tolerance

    # Check that simplified geometry still overlaps original
    assert simplified[0].intersects(poly), "Simplification should preserve spatial overlap"


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

    result = buffer_processor.create_buffer_with_geopandas(
        input_gas_lines_path=str(gas_lines_path),
        buffer_distance_ft=328.084,
        parks_path=None,
        use_multiprocessing=False,
    )

    # Assert no invalid or empty geometries remain
    assert all(result.geometry.is_valid), "Final output still contains invalid geometries"
    assert not any(result.geometry.is_empty), "Final output still contains empty geometries"


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
        merge_buffers_into_planning_file(
            str(buffer_fp),
            str(future_fp),
            point_buffer_distance=10.0
        )
    logger.info("Completed test_merge_future_dev_mixed_geometry")


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


def square(x):
    return x * x

def test_parallel_process_returns_expected():
    """
    Test parallel_process runs the function on a list of inputs and returns expected results.
    """
    logger.info("Running test_parallel_process_returns_expected")
    inputs = [1, 2, 3, 4]
    expected = [1, 4, 9, 16]
    results = parallel_process(square, inputs)
    assert results == expected
    logger.info("test_parallel_process_returns_expected passed.")


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
