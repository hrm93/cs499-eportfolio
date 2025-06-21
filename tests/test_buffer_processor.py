# test buffer_processor:

import logging


import pytest
import geopandas as gpd
from shapely.geometry import Point, Polygon

from gis_tool.buffer_processor import merge_buffers_into_planning_file

import logging
from shapely.geometry import Polygon, Point
import geopandas as gpd
import pytest

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Enable DEBUG level to capture detailed logs for troubleshooting

# Shared test geometry: a simple square polygon for buffer testing
SQUARE_POLY = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


def create_buffer_polygons():
    """
    Helper function to create a GeoDataFrame containing a single square polygon.

    Returns
    -------
    geopandas.GeoDataFrame
        A GeoDataFrame with one polygon feature, CRS set to EPSG:4326.
    """
    return gpd.GeoDataFrame({'id': [1], 'geometry': [SQUARE_POLY]}, crs="EPSG:4326")


def test_merge_buffers_into_planning_file_empty_future_dev(tmp_path):
    """
    Test merging buffer polygons into an empty future development shapefile.

    Scenario:
    - The future development file contains no features.
    - The merged output should equal just the buffer polygons (no additions).

    Validates:
    - The merged GeoDataFrame is not empty.
    - The merged GeoDataFrame length equals the buffer polygons count.
    - The CRS of the merged GeoDataFrame matches the buffer polygons CRS.
    """
    logger.info("Running test_merge_buffers_into_planning_file_empty_future_dev")

    # Create test GeoDataFrames for buffers and empty future dev
    buffer_polygons = gpd.GeoDataFrame({'id': [1], 'geometry': [SQUARE_POLY]}, crs="EPSG:4326")
    empty_future = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    # Save these to temporary files
    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "empty_future.shp"

    buffer_polygons.to_file(str(buffer_fp))
    empty_future.to_file(str(future_fp))

    # Call the merge function with the paths and verify results
    merged_gdf = merge_buffers_into_planning_file(
        str(buffer_fp),
        str(future_fp),
        point_buffer_distance=10.0,
        output_crs=buffer_polygons.crs  # Ensure output CRS matches input buffer CRS
    )

    # Assert conditions
    assert not merged_gdf.empty
    assert len(merged_gdf) == len(buffer_polygons)
    assert merged_gdf.crs == buffer_polygons.crs

    logger.info("test_merge_buffers_into_planning_file_empty_future_dev passed.")


def test_merge_buffers_crs_consistency(tmp_path):
    """
    Test that coordinate reference system (CRS) is preserved correctly after merging.

    Validates:
    - The merged GeoDataFrame CRS is EPSG:4326.
    - The input buffer and future development GeoDataFrames retain their original CRS.
    """
    logger.info("Running test_merge_buffers_crs_consistency")

    buffer_polygons = create_buffer_polygons()

    future_points = gpd.GeoDataFrame(
        {'id': [10], 'geometry': [Point(2, 2)]},
        crs="EPSG:4326"
    )

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"

    buffer_polygons.to_file(str(buffer_fp))
    future_points.to_file(str(future_fp))

    merged_gdf = merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=10.0)

    # Assert CRS consistency post-merge
    assert merged_gdf.crs.to_string() == "EPSG:4326"
    assert buffer_polygons.crs.to_string() == "EPSG:4326"
    assert future_points.crs.to_string() == "EPSG:4326"

    logger.info("test_merge_buffers_crs_consistency passed.")


def test_merge_buffers_into_planning_file(tmp_path):
    """
    Test merging buffer polygons into a future development shapefile.

    Steps:
    - Create temporary shapefiles for buffers and future development points.
    - Call the merge function.
    - Read back the future development shapefile.
    - Verify the merged result contains both input features.
    - Confirm all geometries are polygons or multipolygons.
    """
    logger.info("Running test_merge_buffers_into_planning_file")

    buffer_polygons = create_buffer_polygons()
    future_points = gpd.GeoDataFrame(
        {'id': [10], 'geometry': [Point(2, 2)]},
        crs="EPSG:4326"
    )

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"

    # Save inputs to disk as ESRI Shapefile format
    buffer_polygons.to_file(str(buffer_fp), driver='ESRI Shapefile')
    future_points.to_file(str(future_fp), driver='ESRI Shapefile')
    logger.debug(f"Created shapefiles at {buffer_fp} and {future_fp}")

    # Perform merge operation
    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=10.0)

    # Read back the merged future development shapefile
    merged_gdf = gpd.read_file(future_fp)
    logger.debug(f"Merged GeoDataFrame has {len(merged_gdf)} features.")

    # Assert that the merged result contains both input geometries
    assert len(merged_gdf) == 2

    # Assert all geometries are polygons or multipolygons (buffer output)
    assert all(geom in ['Polygon', 'MultiPolygon'] for geom in merged_gdf.geom_type)

    logger.info("test_merge_buffers_into_planning_file passed.")


def test_merge_empty_buffer_file(tmp_path):
    """
    Test that merging with an empty buffer shapefile:
    - Does not overwrite the future development file.
    - Returns the original future development data unchanged.
    """
    logger.info("Starting test_merge_empty_buffer_file to ensure no overwrite on empty buffers.")

    empty_buffer = gpd.GeoDataFrame(geometry=[], crs="EPSG:32633")
    future_dev = gpd.GeoDataFrame(geometry=[Point(2, 2)], crs="EPSG:32633")

    buffer_fp = tmp_path / "empty_buffer.geojson"
    future_fp = tmp_path / "future_dev.shp"

    # Write empty buffer as GeoJSON to simulate empty input file
    empty_buffer.to_file(str(buffer_fp), driver="GeoJSON")
    future_dev.to_file(str(future_fp))

    # Read the original future dev file to compare after merge attempt
    original_future = gpd.read_file(future_fp)

    logger.info("Calling merge_buffers_into_planning_file with empty buffer file.")
    merged_gdf = merge_buffers_into_planning_file(
        str(buffer_fp), str(future_fp), point_buffer_distance=10.0
    )
    logger.debug("Merge function executed.")

    after_merge_future = gpd.read_file(future_fp)

    # The future development shapefile should be unchanged
    assert not merged_gdf.empty
    assert len(merged_gdf) == len(original_future)
    assert after_merge_future.equals(original_future), (
        "Future development file should remain unchanged if buffer is empty."
    )

    # CRS consistency check
    assert merged_gdf.crs == future_dev.crs

    logger.info("test_merge_empty_buffer_file passed.")


def test_merge_buffer_non_polygon(tmp_path):
    """
    Test that a ValueError is raised when buffer shapefile contains non-polygon geometries.

    This ensures the merge function validates geometry types before proceeding.
    """
    logger.info("Starting test_merge_buffer_non_polygon")

    # Create a GeoDataFrame with a Point geometry instead of Polygon
    gdf = gpd.GeoDataFrame(geometry=[Point(1, 1)], crs="EPSG:32633")
    buffer_fp = tmp_path / "nonpoly_buffer.shp"
    gdf.to_file(str(buffer_fp))
    logger.debug(f"Created buffer shapefile with non-polygon geometry at {buffer_fp}")

    future_gdf = gpd.GeoDataFrame(geometry=[Point(5, 5)], crs="EPSG:32633")
    future_fp = tmp_path / "future_dev.shp"
    future_gdf.to_file(str(future_fp))
    logger.debug(f"Created future development shapefile at {future_fp}")

    # The merge function should raise a ValueError for invalid buffer geometry types
    with pytest.raises(ValueError, match="polygon or multipolygon"):
        logger.info("Calling merge_buffers_into_planning_file expecting ValueError due to non-polygon geometry")
        merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)

    logger.info("Completed test_merge_buffer_non_polygon")


def test_merge_saves_to_geojson(tmp_path):
    """
    Test that merged output can be saved to a GeoJSON file correctly.

    This verifies that the merge function supports output files other than shapefiles.
    """
    logger.info("Starting test_merge_saves_to_geojson")

    buffer = gpd.GeoDataFrame(geometry=[Point(1, 1).buffer(1)], crs="EPSG:4326")
    future_dev = gpd.GeoDataFrame(geometry=[Point(2, 2)], crs="EPSG:4326")

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.geojson"  # Using GeoJSON format for future dev output

    buffer.to_file(str(buffer_fp))
    future_dev.to_file(str(future_fp), driver="GeoJSON")
    logger.debug(f"Created buffer shapefile at {buffer_fp} and future GeoJSON at {future_fp}")

    # Call merge function with GeoJSON output target
    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)
    logger.info("Called merge_buffers_into_planning_file to merge and save GeoJSON output")

    result = gpd.read_file(future_fp)
    logger.debug(f"Read merged GeoJSON file, features count: {len(result)}")

    # Basic assertions on the output file existence and content
    assert future_fp.exists()
    assert len(result) == 2

    logger.info("Completed test_merge_saves_to_geojson")
