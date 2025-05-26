# test buffer_processor:
import os
import geopandas as gpd
import shapely.geometry
from shapely.geometry import Point, Polygon
from shapely.geometry.base import BaseGeometry
import tempfile
import pytest
from gis_tool.buffer_processor import fix_geometry, create_buffer_with_geopandas, merge_buffers_into_planning_file, \
    ensure_projected_crs, subtract_parks_from_buffer
from gis_tool import config
from unittest.mock import Mock
import logging

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


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


def test_fix_geometry_valid():
    """
    Test that a valid geometry is returned unchanged by fix_geometry.
    """
    logger.info("Running test_fix_geometry_valid")
    pt = Point(0, 0)
    assert fix_geometry(pt) == pt
    logger.info("test_fix_geometry_valid passed.")


def test_fix_geometry_invalid():
    """
    Test that fix_geometry attempts to fix an invalid self-intersecting polygon.
    The fixed geometry should be valid and not None.
    """
    logger.info("Running test_fix_geometry_invalid")
    invalid_poly = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
    logger.debug(f"Invalid polygon is_valid: {invalid_poly.is_valid}")
    assert not invalid_poly.is_valid

    fixed = fix_geometry(invalid_poly)
    logger.debug(f"Fixed polygon validity: {fixed.is_valid if fixed else 'None'}")
    assert fixed is not None
    assert fixed.is_valid
    logger.info("test_fix_geometry_invalid passed.")


def test_fix_geometry_unfixable():
    """
    Test that fix_geometry returns None if buffering to fix geometry
    raises an exception (simulates unfixable geometry).
    """
    logger.info("Running test_fix_geometry_unfixable")
    mock_geom = Mock(spec=BaseGeometry)
    mock_geom.is_valid = False
    mock_geom.buffer.side_effect = Exception("Cannot buffer")

    result = fix_geometry(mock_geom)
    logger.debug(f"Result from fix_geometry: {result}")
    assert result is None
    logger.info("test_fix_geometry_unfixable passed.")

def test_ensure_projected_crs_already_projected():
    """
    Test that a projected CRS is returned unchanged by ensure_projected_crs.
    """
    logger.info("Running test_ensure_projected_crs_already_projected")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=config.DEFAULT_CRS)
    projected = ensure_projected_crs(gdf)
    logger.debug(f"Original CRS: {gdf.crs}, Projected CRS: {projected.crs}")
    assert projected.crs.to_string() == config.DEFAULT_CRS
    logger.info("test_ensure_projected_crs_already_projected passed.")


def test_ensure_projected_crs_needs_reproject():
    """
    Test that a geographic CRS (EPSG:4326) is reprojected by ensure_projected_crs.
    """
    logger.info("Running test_ensure_projected_crs_needs_reproject")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=config.GEOGRAPHIC_CRS)
    projected = ensure_projected_crs(gdf)
    logger.debug(f"Original CRS: {gdf.crs}, Projected CRS: {projected.crs}")
    assert projected.crs != gdf.crs
    assert projected.crs.is_projected
    logger.info("test_ensure_projected_crs_needs_reproject passed.")

def test_create_buffer_with_missing_crs():
    """
    Test create_buffer_with_geopandas handles input files missing CRS by assigning default.
    """
    logger.info("Running test_create_buffer_with_missing_crs")
    with tempfile.TemporaryDirectory() as tmpdir:
        gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=config.GEOGRAPHIC_CRS)
        input_path = os.path.join(tmpdir, "no_crs_input.shp")
        gdf.to_file(input_path)
        logger.debug(f"Created shapefile with missing CRS at {input_path}")

        buffered_gdf = create_buffer_with_geopandas(input_path, buffer_distance_ft=config.DEFAULT_BUFFER_DISTANCE_FT)

        logger.debug(f"Buffered GeoDataFrame CRS: {buffered_gdf.crs}")
        assert buffered_gdf.crs is not None
        assert not buffered_gdf.empty
    logger.info("test_create_buffer_with_missing_crs passed.")


def test_merge_missing_crs_inputs(tmp_path):
    """
    Test merge behavior when one or both files lack a CRS.
    """
    logger.info("Running test_merge_missing_crs_inputs")
    buffer = gpd.GeoDataFrame(geometry=[Point(5, 5).buffer(5)], crs="EPSG:4326")
    future_dev = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:4326")

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"
    buffer.to_file(str(buffer_fp))
    future_dev.to_file(str(future_fp))
    logger.debug(f"Buffer and future development shapefiles saved at {buffer_fp} and {future_fp}")

    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)
    result = gpd.read_file(future_fp)

    logger.debug(f"Merged shapefile CRS: {result.crs}, feature count: {len(result)}")
    assert not result.empty
    assert result.crs is not None
    logger.info("test_merge_missing_crs_inputs passed.")


def test_subtract_parks_from_buffer(tmp_path):
    """
     Test that subtract_parks_from_buffer correctly subtracts park polygons
     from buffer polygons.

     This test creates a simple square buffer polygon and a smaller overlapping
     park polygon, writes the park polygon to a temporary GeoJSON file, and
     verifies that the resulting buffered polygon has a 'hole' where the park
     polygon overlapped.

     Assertions:
     - The result GeoDataFrame is not empty.
     - The CRS remains unchanged after subtraction.
     - Each resulting geometry is valid, non-empty, and has an area reflecting
       the subtraction of the overlapping park polygon.
     """
    # Create simple buffer polygon (square)
    buffer_poly = shapely.geometry.box(0, 0, 10, 10)
    buffer_gdf = gpd.GeoDataFrame(geometry=[buffer_poly], crs="EPSG:3857")

    # Create park polygon overlapping part of buffer (smaller square inside)
    park_poly = shapely.geometry.box(5, 5, 15, 15)
    parks_gdf = gpd.GeoDataFrame(geometry=[park_poly], crs="EPSG:3857")

    # Save parks to temporary GeoJSON file
    parks_path = tmp_path / "parks.geojson"
    parks_gdf.to_file(str(parks_path), driver="GeoJSON")

    # Call function under test
    result = subtract_parks_from_buffer(buffer_gdf, str(parks_path))

    # Basic assertions
    assert not result.empty, "Resulting GeoDataFrame should not be empty."
    assert result.crs == buffer_gdf.crs, "CRS should remain the same after subtraction."

    # Check area reduction due to subtraction
    expected_area = buffer_poly.area - buffer_poly.intersection(park_poly).area
    for geom in result.geometry:
        assert geom.is_valid, "Geometry should be valid."
        assert not geom.is_empty, "Geometry should not be empty."
        assert abs(geom.area - expected_area) < 1e-6, "Geometry area should reflect subtraction."


def test_create_buffer_with_geopandas(sample_gdf):
    """
    Test `create_buffer_with_geopandas` function by:
    - Writing sample_gdf to a temporary shapefile.
    - Calling the buffer creation function with 25 ft buffer distance.
    - Saving the buffered GeoDataFrame to a shapefile.
    - Asserting the output shapefile exists and is not empty.
    """
    logger.info("Running test_create_buffer_with_geopandas")
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.shp")
        sample_gdf.to_file(input_path)
        logger.debug(f"Sample GeoDataFrame saved at {input_path}")

        buffered_gdf = create_buffer_with_geopandas(
            input_path,
            buffer_distance_ft=25
        )

        output_path = os.path.join(tmpdir, "buffered_output.shp")
        buffered_gdf.to_file(output_path)
        logger.debug(f"Buffered GeoDataFrame saved at {output_path}")

        assert os.path.exists(output_path)
        out_gdf = gpd.read_file(output_path)
        assert not out_gdf.empty
    logger.info("test_create_buffer_with_geopandas passed.")

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
    # Create empty buffer GeoDataFrame
    empty_buffer = gpd.GeoDataFrame(geometry=[], crs="EPSG:32633")
    # Create future development GeoDataFrame with one point
    future_dev = gpd.GeoDataFrame(geometry=[Point(2, 2)], crs="EPSG:32633")

    buffer_fp = tmp_path / "empty_buffer.geojson"
    future_fp = tmp_path / "future_dev.shp"

    # Write empty buffer as GeoJSON (empty file)
    empty_buffer.to_file(str(buffer_fp), driver="GeoJSON")
    # Write future development shapefile (non-empty)
    future_dev.to_file(str(future_fp))

    # Read original future development file contents for later comparison
    original_future = gpd.read_file(future_fp)

    # Call merge; this should skip writing empty merged file and return merged GeoDataFrame
    merged_gdf = merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=10.0)

    # After merge, read the future file again
    after_merge_future = gpd.read_file(future_fp)

    # Assert merged GeoDataFrame is not empty and matches the original future dev data (unchanged)
    assert not merged_gdf.empty
    assert len(merged_gdf) == len(original_future)
    assert after_merge_future.equals(original_future), "Future dev file should not be overwritten if buffer is empty"

    # Confirm merged_gdf has same CRS as future_dev
    assert merged_gdf.crs == future_dev.crs


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
