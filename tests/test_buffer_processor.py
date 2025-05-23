# test buffer_processor:
import os
import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon, MultiPolygon
import tempfile
import pytest
from gis_tool import buffer_processor
from gis_tool.buffer_processor import fix_geometry, create_buffer_with_geopandas, merge_buffers_into_planning_file


def test_fix_geometry_valid():
    """
    Test that a valid geometry is returned unchanged by fix_geometry.
    """
    pt = Point(0, 0)
    assert fix_geometry(pt) == pt


def test_fix_geometry_invalid():
    """
    Test that fix_geometry attempts to fix an invalid self-intersecting polygon.
    The fixed geometry should be valid and not None.
    """
    invalid_poly = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
    assert not invalid_poly.is_valid

    fixed = fix_geometry(invalid_poly)
    assert fixed is not None
    assert fixed.is_valid


def test_fix_geometry_unfixable(monkeypatch):
    """
    Test that fix_geometry returns None if buffering to fix geometry
    raises an exception (simulates unfixable geometry).
    """
    class BadGeom:
        is_valid = False
        def buffer(self, width):
            raise Exception("Cannot buffer")

    bad_geom = BadGeom()
    result = fix_geometry(bad_geom)
    assert result is None


@pytest.fixture
def sample_gdf():
    """
    Fixture that returns a sample GeoDataFrame with one point feature.

    The GeoDataFrame uses EPSG:32633 projected CRS suitable for metric buffering.
    """
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
    return gdf


def test_create_buffer_with_geopandas(sample_gdf):
    """
    Test `create_buffer_with_geopandas` function by:
    - Writing sample_gdf to a temporary shapefile.
    - Calling the buffer creation function with 25 ft buffer distance.
    - Saving the buffered GeoDataFrame to a shapefile.
    - Asserting the output shapefile exists and is not empty.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.shp")
        sample_gdf.to_file(input_path)

        # Call the buffer creation function (returns GeoDataFrame, no file write)
        buffered_gdf = create_buffer_with_geopandas(
            input_path,
            buffer_distance_ft=25
        )

        # Save buffered GeoDataFrame for verification
        output_path = os.path.join(tmpdir, "buffered_output.shp")
        buffered_gdf.to_file(output_path)

        # Validate output file exists
        assert os.path.exists(output_path)

        # Reload and check output GeoDataFrame is not empty
        out_gdf = gpd.read_file(output_path)
        assert not out_gdf.empty


@pytest.fixture
def sample_future_development():
    """
    Fixture creating a simple Future Development shapefile with a point feature and CRS.

    Uses a temporary directory and yields the file path for test use.
    """
    tmpdir = tempfile.TemporaryDirectory()
    gdf = gpd.GeoDataFrame(
        {"geometry": [Point(100, 100)]},
        crs = "EPSG:32633"
    )
    path = os.path.join(tmpdir.name, "future_dev.shp")
    gdf.to_file(path)
    yield path
    tmpdir.cleanup()


@pytest.fixture
def sample_buffer():
    """
    Fixture creating a buffer shapefile with attributes and polygon geometry.

    Uses a temporary directory and yields the file path for test use.
    """
    tmpdir = tempfile.TemporaryDirectory()
    gdf = gpd.GeoDataFrame(
        {
            "Name": ["Buffer1"],
            "Date": ["2024-01-01"],
            "PSI": [120],
            "Material": ["steel"],
            "geometry": [Point(100, 100).buffer(10)]},
        crs="EPSG:32633"
    )
    path = os.path.join(tmpdir.name, "buffer.shp")
    gdf.to_file(path)
    yield path
    tmpdir.cleanup()


def test_merge_buffers_into_planning_file(tmp_path):
    """
    Test merging buffer polygons into a Future Development planning shapefile by:
    - Creating temporary buffer polygons and future development shapefiles.
    - Calling `merge_buffers_into_planning_file` with buffer and future dev paths.
    - Asserting merged GeoDataFrame contains features from both inputs.
    - Validating geometry types are polygons or multipolygons.
    """
    # Create buffer polygon GeoDataFrame (square polygon)
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])  # square polygon
    buffer_polygons = gpd.GeoDataFrame({'id': [1], 'geometry': [poly]}, crs="EPSG:4326")

    # Create future development points GeoDataFrame
    future_points = gpd.GeoDataFrame(
        {
        'id': [10],
        'geometry': [Point(2, 2)]},
        crs="EPSG:4326"
    )
    # Save temp shapefiles
    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"
    buffer_polygons.to_file(str(buffer_fp), driver='ESRI Shapefile')
    future_points.to_file(str(future_fp), driver='ESRI Shapefile')

    # Run merge function with 10 meter buffer distance for converting points
    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=10.0)

    # Read merged output (future_fp is overwritten)
    merged_gdf = gpd.read_file(future_fp)

    # Assert merged result has 2 features (original + buffer)
    assert len(merged_gdf) == 2

    # Assert all geometries are polygons or multipolygons
    assert all(geom in ['Polygon', 'MultiPolygon'] for geom in merged_gdf.geom_type)
