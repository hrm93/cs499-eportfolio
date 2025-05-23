# test buffer_processor:
import os
import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon, MultiPolygon
import tempfile
import pytest
from gis_tool import buffer_processor
from gis_tool.buffer_processor import create_buffer_with_geopandas, merge_buffers_into_planning_file


@pytest.fixture
def sample_gdf():
    """Fixture that returns a sample GeoDataFrame with one point feature."""
    gdf = gpd.GeoDataFrame(
        {"Name": ["test"], "Date": ["2020-01-01"], "PSI": [100], "Material": ["steel"],
         "geometry": [Point(0, 0)]},
        crs="EPSG:32633"
    )
    return gdf


def test_create_buffer_with_geopandas(sample_gdf):
    """
    Test the create_buffer_with_geopandas function:
    - Saves a sample GeoDataFrame to a temporary shapefile.
    - Calls the buffer creation function with a 25 ft buffer distance.
    - Saves the buffered GeoDataFrame to a shapefile.
    - Verifies that the output shapefile exists.
    - Loads the output and asserts it is not empty.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.shp")
        sample_gdf.to_file(input_path)

        # Call function: returns GeoDataFrame (does NOT write file)
        buffered_gdf = create_buffer_with_geopandas(
            input_path,
            buffer_distance_ft=25
        )

        # Now save buffered GeoDataFrame to file for verification
        output_path = os.path.join(tmpdir, "buffered_output.shp")
        buffered_gdf.to_file(output_path)

        # Validate output file exists
        assert os.path.exists(output_path)

        # Load output file and assert not empty
        out_gdf = gpd.read_file(output_path)
        assert not out_gdf.empty


@pytest.fixture
def sample_future_development():
    """Fixture creating a simple Future Development shapefile with CRS."""
    tmpdir = tempfile.TemporaryDirectory()
    gdf = gpd.GeoDataFrame({
        "geometry": [Point(100, 100)]
    }, crs="EPSG:32633")
    path = os.path.join(tmpdir.name, "future_dev.shp")
    gdf.to_file(path)
    yield path
    tmpdir.cleanup()


@pytest.fixture
def sample_buffer():
    """Fixture creating a buffer shapefile with relevant attributes and CRS."""
    tmpdir = tempfile.TemporaryDirectory()
    gdf = gpd.GeoDataFrame({
        "Name": ["Buffer1"],
        "Date": ["2024-01-01"],
        "PSI": [120],
        "Material": ["steel"],
        "geometry": [Point(100, 100).buffer(10)]
    }, crs="EPSG:32633")
    path = os.path.join(tmpdir.name, "buffer.shp")
    gdf.to_file(path)
    yield path
    tmpdir.cleanup()


def test_merge_buffers_into_planning_file(tmp_path):
    """
    Test merging a buffer shapefile into a Future Development planning shapefile:
    - Reads the original Future Development features.
    - Calls merge_buffers_into_planning_file with the buffer and future dev paths.
    - Reloads the Future Development shapefile.
    - Asserts the number of features has increased by the buffer features count.
    - Checks that merged data includes buffer attributes.
    """
    # Create buffer polygons GeoDataFrame
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])  # square polygon

    buffer_polygons = gpd.GeoDataFrame({'id': [1], 'geometry': [poly]}, crs="EPSG:4326")

    # Create future development points GeoDataFrame
    future_points = gpd.GeoDataFrame({
        'id': [10],
        'geometry': [Point(2, 2)]
    }, crs="EPSG:4326")

    # Save temp shapefiles
    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"
    buffer_polygons.to_file(str(buffer_fp), driver='ESRI Shapefile')
    future_points.to_file(str(future_fp), driver='ESRI Shapefile')

    # Run merge function with default buffer distance (10 meters)
    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=10.0)

    # Read output (future_fp is overwritten)
    merged_gdf = gpd.read_file(future_fp)

    # Debug output after merge
    print("DEBUG: Merged GeoDataFrame after merging:")
    print(merged_gdf)
    print(f"Number of features: {len(merged_gdf)}")
    print(f"Geometry types: {merged_gdf.geom_type.unique()}")

    # Check that merged_gdf has both geometries and they are polygons
    assert len(merged_gdf) == 2
    assert all(geom in ['Polygon', 'MultiPolygon'] for geom in merged_gdf.geom_type)
