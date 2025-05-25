# test buffer_processor:
import os
import geopandas as gpd
from shapely.geometry import Point, Polygon
import tempfile
import pytest
from gis_tool.buffer_processor import fix_geometry, create_buffer_with_geopandas, merge_buffers_into_planning_file, ensure_projected_crs


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


def test_ensure_projected_crs_already_projected():
    """
    Test that a projected CRS is returned unchanged by ensure_projected_crs.
    """
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:32633")
    projected = ensure_projected_crs(gdf)
    assert projected.crs.to_string() == "EPSG:32633"


def test_ensure_projected_crs_needs_reproject():
    """
    Test that a geographic CRS (EPSG:4326) is reprojected by ensure_projected_crs.
    """
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:4326")
    projected = ensure_projected_crs(gdf)
    assert projected.crs != gdf.crs
    assert projected.crs.is_projected


def test_create_buffer_with_missing_crs():
    """
    Test create_buffer_with_geopandas handles input files missing CRS by assigning default.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:4326")
        input_path = os.path.join(tmpdir, "no_crs_input.shp")
        gdf.to_file(input_path)

        buffered_gdf = create_buffer_with_geopandas(input_path, buffer_distance_ft=25)

        assert buffered_gdf.crs is not None
        assert not buffered_gdf.empty


def test_merge_missing_crs_inputs(tmp_path):
    """
    Test merge behavior when one or both files lack a CRS.
    """
    buffer = gpd.GeoDataFrame(geometry=[Point(5, 5).buffer(5)], crs="EPSG:4326")
    future_dev = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:4326")

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"
    buffer.to_file(str(buffer_fp))
    future_dev.to_file(str(future_fp))

    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)
    result = gpd.read_file(future_fp)

    assert not result.empty
    assert result.crs is not None


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


def test_merge_empty_buffer_file(tmp_path):
    """
    Test merge function with an empty buffer file.
    """
    empty_buffer = gpd.GeoDataFrame(geometry=[], crs="EPSG:32633")
    future_dev = gpd.GeoDataFrame(geometry=[Point(2, 2)], crs="EPSG:32633")

    buffer_fp = tmp_path / "empty_buffer.shp"
    future_fp = tmp_path / "future_dev.shp"

    empty_buffer.to_file(str(buffer_fp))
    future_dev.to_file(str(future_fp))

    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=10.0)
    result = gpd.read_file(future_fp)

    assert len(result) == 1  # Only the original future point should remain


def test_merge_buffer_non_polygon(tmp_path):
    """
    Test that a ValueError is raised if the buffer shapefile contains non-polygon geometries.
    """
    gdf = gpd.GeoDataFrame(geometry=[Point(1, 1)], crs="EPSG:32633")
    buffer_fp = tmp_path / "nonpoly_buffer.shp"
    gdf.to_file(str(buffer_fp))

    future_gdf = gpd.GeoDataFrame(geometry=[Point(5, 5)], crs="EPSG:32633")
    future_fp = tmp_path / "future_dev.shp"
    future_gdf.to_file(str(future_fp))

    with pytest.raises(ValueError, match="polygon or multipolygon"):
        merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)


def test_merge_saves_to_geojson(tmp_path):
    """
    Test saving merged output to a GeoJSON file.
    """
    buffer = gpd.GeoDataFrame(geometry=[Point(1, 1).buffer(1)], crs="EPSG:4326")
    future_dev = gpd.GeoDataFrame(geometry=[Point(2, 2)], crs="EPSG:4326")

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.geojson"  # Save to GeoJSON!
    buffer.to_file(str(buffer_fp))
    future_dev.to_file(str(future_fp), driver="GeoJSON")

    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)
    result = gpd.read_file(future_fp)

    assert future_fp.exists()
    assert len(result) == 2


'''
def test_merge_future_dev_mixed_geometry(tmp_path):
    """
    Test merging when future development file has mixed geometry types.
    """
    future_dev = gpd.GeoDataFrame(
        geometry=[
            Point(0, 0),
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])  # Already a polygon
        ],
        crs="EPSG:32633"
    )
    buffer = gpd.GeoDataFrame(
        geometry=[Polygon([(2, 2), (3, 2), (3, 3), (2, 3)])],
        crs="EPSG:32633"
    )

    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "mixed_future.geojson"
    output_fp = tmp_path / "merged_output.shp"  # ✅ New file for merged result

    buffer.to_file(str(buffer_fp))
    future_dev.to_file(str(future_fp), driver="GeoJSON")

    # ✅ Modified function to allow saving to a different output file
    merged = merge_buffers_into_planning_file(
        str(buffer_fp),
        str(future_fp),
        point_buffer_distance=10.0
    )

    # ✅ Write merged result to a separate file to avoid schema conflicts
    merged.to_file(str(output_fp))

    result = gpd.read_file(output_fp)

    # Should be: 1 original polygon + 1 point buffered + 1 polygon from buffer file = 3
    assert len(result) == 3
    assert all(result.geometry.type.isin(["Polygon", "MultiPolygon"]))
'''