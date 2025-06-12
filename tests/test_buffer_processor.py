# test buffer_processor:

import logging


import pytest
import geopandas as gpd
from shapely.geometry import Point, Polygon

from gis_tool.buffer_processor import merge_buffers_into_planning_file

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs

# Shared test geometry
SQUARE_POLY = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])


# Helper to create the shared buffer polygons GeoDataFrame
def create_buffer_polygons():
    return gpd.GeoDataFrame({'id': [1], 'geometry': [SQUARE_POLY]}, crs="EPSG:4326")


def test_merge_buffers_into_planning_file_empty_future_dev(tmp_path):
    """
    Test merging buffers when future development shapefile is empty.
    The merged result should be equal to buffer polygons only.
    """
    logger.info("Running test_merge_buffers_into_planning_file_empty_future_dev")

    buffer_polygons = gpd.GeoDataFrame({'id': [1], 'geometry': [SQUARE_POLY]}, crs="EPSG:4326")
    empty_future = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "empty_future.shp"

    buffer_polygons.to_file(str(buffer_fp))
    empty_future.to_file(str(future_fp))

    merged_gdf = merge_buffers_into_planning_file(
        str(buffer_fp),
        str(future_fp),
        point_buffer_distance=10.0,
        output_crs=buffer_polygons.crs  # REQUEST output CRS to match buffer CRS
    )

    assert not merged_gdf.empty
    assert len(merged_gdf) == len(buffer_polygons)
    assert merged_gdf.crs == buffer_polygons.crs  # Should pass now

    logger.info("test_merge_buffers_into_planning_file_empty_future_dev passed.")


def test_merge_buffers_crs_consistency(tmp_path):
    """
    Test that CRS is preserved correctly after merging buffers into future development shapefile.
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

    # Check that the merged output uses projected CRS (EPSG:32633) as intended
    assert merged_gdf.crs.to_string() == "EPSG:32633"

    # Check that the original inputs remain in their original CRS
    assert buffer_polygons.crs.to_string() == "EPSG:4326"
    assert future_points.crs.to_string() == "EPSG:4326"

    logger.info("test_merge_buffers_crs_consistency passed.")


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
    buffer_polygons = create_buffer_polygons()

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
