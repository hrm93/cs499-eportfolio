# test buffer_processor:
import os
import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon
from shapely.geometry.base import BaseGeometry
import tempfile
import pytest
from gis_tool import buffer_processor
from gis_tool.buffer_processor import fix_geometry, create_buffer_with_geopandas, merge_buffers_into_planning_file, \
    ensure_projected_crs
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


@pytest.fixture
def sample_gas_lines_gdf():
    # Create a GeoDataFrame with LineStrings (simulating gas lines)
    geoms = [LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])]
    gdf = gpd.GeoDataFrame({'geometry': geoms}, crs="EPSG:3857")
    return gdf


@pytest.fixture
def sample_parks_gdf():
    # Create a GeoDataFrame with Polygons (simulating parks)
    geoms = [Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)])]
    gdf = gpd.GeoDataFrame({'geometry': geoms}, crs="EPSG:3857")
    return gdf


@pytest.fixture
def sample_parks_file(tmp_path, sample_parks_gdf):
    file_path = tmp_path / "parks.geojson"
    sample_parks_gdf.to_file(str(file_path), driver='GeoJSON')
    return str(file_path)


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


def assert_geodataframes_equal(gdf1, gdf2, tol=1e-6):
    assert isinstance(gdf1, gpd.GeoDataFrame)
    assert isinstance(gdf2, gpd.GeoDataFrame)
    assert gdf1.crs == gdf2.crs
    assert len(gdf1) == len(gdf2)
    for geom1, geom2 in zip(gdf1.geometry, gdf2.geometry):
        assert geom1.equals_exact(geom2, tolerance=tol)


def test_subtract_parks_from_buffer_multiprocessing(sample_gas_lines_gdf, sample_parks_file):
    # Buffer the gas lines (non-multiprocessing)
    buffer_distance = 100  # meters
    buffered_gdf = sample_gas_lines_gdf.copy()
    buffered_gdf['geometry'] = buffered_gdf.geometry.buffer(buffer_distance)

    # Test subtract_parks_from_buffer without multiprocessing
    result_serial = buffer_processor.subtract_parks_from_buffer(buffered_gdf.copy(), sample_parks_file, use_multiprocessing=False)

    # Test subtract_parks_from_buffer with multiprocessing
    result_parallel = buffer_processor.subtract_parks_from_buffer(buffered_gdf.copy(), sample_parks_file, use_multiprocessing=True)

    # Use helper assertion
    assert_geodataframes_equal(result_serial, result_parallel)


def test_create_buffer_with_geopandas_multiprocessing(tmp_path, sample_gas_lines_gdf, sample_parks_file):
    gas_lines_path = tmp_path / "gas_lines.geojson"
    sample_gas_lines_gdf.to_file(str(gas_lines_path), driver='GeoJSON')

    # Test without multiprocessing
    result_serial = buffer_processor.create_buffer_with_geopandas(
        input_gas_lines_path=str(gas_lines_path),
        buffer_distance_ft=328.084,  # 100 meters in feet
        parks_path=sample_parks_file,
        use_multiprocessing=False,
    )

    # Test with multiprocessing
    result_parallel = buffer_processor.create_buffer_with_geopandas(
        input_gas_lines_path=str(gas_lines_path),
        buffer_distance_ft=328.084,
        parks_path=sample_parks_file,
        use_multiprocessing=True,
    )

    # Use helper assertion
    assert_geodataframes_equal(result_serial, result_parallel)


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
