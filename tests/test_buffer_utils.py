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
logger.setLevel(logging.DEBUG)  # Enable DEBUG level to capture detailed logs for troubleshooting


def test_buffer_geometry_simple():
    """
    Test buffering a simple Point geometry using buffer_geometry.

    Validates:
    - Resulting geometry has positive area (non-empty buffer).
    - The original point lies within the buffered geometry.
    """
    logger.info("Running test_buffer_geometry_simple")

    point = Point(1, 1)
    buffer_dist = 10

    buffered = buffer_geometry(point, buffer_dist)

    # Assert buffer has positive area
    assert buffered.area > 0

    # Assert original point is contained within the buffer
    assert buffered.contains(point)

    logger.info("test_buffer_geometry_simple passed.")


def test_buffer_geometry_helper_consistency():
    """
    Test that buffer_geometry_helper returns the same result as buffer_geometry.

    Assumes:
    - buffer_geometry_helper accepts a tuple (geometry, distance).
    - buffer_geometry_helper uses the same buffer distance internally.

    Validates:
    - The two functions produce geometrically equivalent outputs.
    """
    logger.info("Running test_buffer_geometry_helper_consistency")

    geom = Point(0, 0)
    fixed_dist = 10

    result_main = buffer_geometry(geom, fixed_dist)
    result_helper = buffer_geometry_helper((geom, fixed_dist))  # tuple input expected by helper

    # Assert geometric equality of results
    assert result_main.equals(result_helper)

    logger.info("test_buffer_geometry_helper_consistency passed.")


def test_subtract_park_from_geom_difference():
    """
    Test that subtract_park_from_geom correctly subtracts park polygons from a buffer polygon.

    Setup:
    - Create a buffer geometry around a point.
    - Create a smaller park polygon overlapping the buffer.

    Validates:
    - The resulting geometry area is smaller than the original buffer's area.
    """
    logger.info("Running test_subtract_park_from_geom_difference")

    buffer_geom = Point(0, 0).buffer(10)
    park_geoms = [Point(0, 0).buffer(5)]  # List of park polygons

    result = subtract_park_from_geom(buffer_geom, park_geoms)

    # The area after subtraction should be less than original
    assert result.area < buffer_geom.area

    logger.info("test_subtract_park_from_geom_difference passed.")


def test_subtract_park_from_geom_helper_equivalence():
    """
    Test subtract_park_from_geom_helper produces the same output as subtract_park_from_geom.

    Setup:
    - Create buffer geometry.
    - Create one or more park geometries.
    - Provide the same inputs to both main and helper functions.

    Validates:
    - The two functions return geometrically equivalent results.
    """
    logger.info("Running test_subtract_park_from_geom_helper_equivalence")

    buffer_geom = Point(0, 0).buffer(8)
    parks_geoms = [Point(1, 1).buffer(3)]

    result_main = subtract_park_from_geom(buffer_geom, parks_geoms)
    result_helper = subtract_park_from_geom_helper((buffer_geom, parks_geoms))

    assert result_main.equals(result_helper)

    logger.info("test_subtract_park_from_geom_helper_equivalence passed.")


def test_merge_future_dev_mixed_geometry(tmp_path):
    """
    Test that merging fails with a ValueError when the future development file contains mixed geometry types.

    Setup:
    - Future development GeoDataFrame contains both Point and Polygon geometries.
    - Buffer GeoDataFrame contains Polygon geometry.

    Validates:
    - merge_buffers_into_planning_file raises ValueError due to mixed geometry types in future dev.
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
    logger.debug(f"Created buffer shapefile and future GeoJSON with mixed geometries")

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

    Setup:
    - Add intentionally invalid and empty geometries to sample gas lines GeoDataFrame.
    - Buffer the gas lines via create_buffer_with_geopandas.
    - Confirm output has only valid, non-empty geometries.
    """
    logger.info("Running test_final_geometry_validation_removes_invalid_and_empty")

    # Create an intentionally invalid self-intersecting polygon
    invalid_geom = Polygon([(0, 0), (1, 1), (1, 0), (0, 0)])  # self-intersecting polygon
    empty_geom = Polygon()  # empty geometry

    # Append invalid and empty geometries to the sample data
    sample_gas_lines_gdf.loc[len(sample_gas_lines_gdf)] = [invalid_geom]
    sample_gas_lines_gdf.loc[len(sample_gas_lines_gdf)] = [empty_geom]

    gas_lines_path = tmp_path / "gas_lines.geojson"
    sample_gas_lines_gdf.to_file(str(gas_lines_path), driver='GeoJSON')

    # Run buffering process on the modified data
    result = gis_tool.buffer_creation.create_buffer_with_geopandas(
        input_gas_lines_path=str(gas_lines_path),
        buffer_distance_ft=328.084,
        parks_path=None,
        use_multiprocessing=False,
    )

    # Assert that no invalid or empty geometries remain in the result
    assert all(result.geometry.is_valid), "Final output still contains invalid geometries"
    assert not any(result.geometry.is_empty), "Final output still contains empty geometries"

    logger.info("test_final_geometry_validation_removes_invalid_and_empty passed.")
