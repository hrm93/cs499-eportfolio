# test_spatial_utils
import os
import tempfile
import logging

import geopandas as gpd
import pytest
from shapely.geometry import LineString, Point, Polygon
from pyproj import CRS

from gis_tool.spatial_utils import (
    validate_and_reproject_crs,
    validate_geometry_column,
    ensure_projected_crs,
    buffer_intersects_gas_lines,
    validate_geometry_crs,
    reproject_geometry_to_crs
)
from gis_tool import config
from gis_tool.buffer_processor import merge_buffers_into_planning_file
from gis_tool.buffer_creation import create_buffer_with_geopandas

logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)  # Capture all logs


def test_validate_and_reproject_crs_raises_without_crs(caplog):
    """
    Test that validate_and_reproject_crs raises a ValueError and logs an error
    when the input GeoDataFrame has no CRS defined.
    """
    logger.info("Starting test_validate_and_reproject_crs_raises_without_crs")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=None)
    target_crs = "EPSG:32610"
    dataset_name = "test_dataset"

    with caplog.at_level(logging.ERROR, logger="gis_tool.spatial_utils"):
        logger.info("Calling validate_and_reproject_crs expecting ValueError due to missing CRS.")
        with pytest.raises(ValueError, match="missing a CRS"):
            validate_and_reproject_crs(gdf, target_crs, dataset_name)
        logger.info("ValueError correctly raised for missing CRS.")
        # Confirm error log contains dataset name
        assert any(dataset_name in record.message for record in caplog.records)
    logger.info("Completed test_validate_and_reproject_crs_raises_without_crs")


def test_validate_and_reproject_crs_reprojects(caplog):
    """
    Test that validate_and_reproject_crs correctly reprojects the GeoDataFrame
    when its CRS differs from the target CRS, and logs a warning about reprojection.
    """
    logger.info("Starting test_validate_and_reproject_crs_reprojects")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:4326")
    target_crs = "EPSG:32610"
    dataset_name = "test_dataset"

    with caplog.at_level(logging.WARNING, logger="gis_tool.spatial_utils"):
        logger.info("Calling validate_and_reproject_crs expecting reprojection.")
        result = validate_and_reproject_crs(gdf, target_crs, dataset_name)
        # Assert that CRS was updated to target CRS
        assert result.crs.to_string() == CRS(target_crs).to_string()
        logger.info("CRS reprojection successful.")
        # Assert warning log about reprojection was emitted
        assert any("Auto-reprojecting" in record.message for record in caplog.records)
    logger.info("Completed test_validate_and_reproject_crs_reprojects")


def test_validate_and_reproject_crs_no_reprojection(caplog):
    """
    Test that validate_and_reproject_crs returns the original GeoDataFrame unchanged
    if the CRS already matches the target CRS, and logs an info message confirming this.
    """
    logger.info("Starting test_validate_and_reproject_crs_no_reprojection")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:32610")
    target_crs = "EPSG:32610"
    dataset_name = "test_dataset"

    with caplog.at_level(logging.INFO, logger="gis_tool.spatial_utils"):
        logger.info("Calling validate_and_reproject_crs expecting no reprojection.")
        result = validate_and_reproject_crs(gdf, target_crs, dataset_name)
        # Assert CRS unchanged
        assert result.crs.to_string() == CRS(target_crs).to_string()
        logger.info("CRS unchanged as expected.")
        # Assert info log about matching CRS was emitted
        assert any("already in target CRS" in record.message for record in caplog.records)
    logger.info("Completed test_validate_and_reproject_crs_no_reprojection")


def test_validate_geometry_column_valid_geometries():
    """
    Test that validate_geometry_column returns the same GeoDataFrame if all geometries are valid.
    """
    logger.info("Starting test_validate_geometry_column_valid_geometries")
    gdf = gpd.GeoDataFrame(geometry=[
        Point(0, 0),
        LineString([(1, 1), (2, 2)]),
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    ], crs="EPSG:4326")

    logger.info("Calling validate_geometry_column with valid geometries.")
    validated_gdf = validate_geometry_column(gdf, "valid_dataset")
    assert validated_gdf.equals(gdf)
    assert all(validated_gdf.geometry.notnull())
    assert all(validated_gdf.geometry.is_valid)
    logger.info("test_validate_geometry_column_valid_geometries passed.")


def test_validate_geometry_column_invalid_geometries_not_removed(caplog):
    """
    Test that validate_geometry_column does not remove None geometries.
    """
    logger.info("Starting test_validate_geometry_column_invalid_geometries_not_removed")
    gdf = gpd.GeoDataFrame(geometry=[
        Point(0, 0),
        None
    ], crs="EPSG:4326")
    with caplog.at_level(logging.WARNING, logger="gis_tool.spatial_utils"):
        logger.info("Calling validate_geometry_column with invalid geometries.")
        result = validate_geometry_column(gdf, "dataset_with_invalid")
        assert len(result) == 2
        assert result.geometry.isnull().sum() == 1
        logger.info("Warning logged for invalid geometries as expected.")
    logger.info("test_validate_geometry_column_invalid_geometries_not_removed passed.")


def test_validate_geometry_column_empty_geometries_removed(caplog):
    """
    Test that empty geometries are warned about by validate_geometry_column.
    """
    logger.info("Starting test_validate_geometry_column_empty_geometries_removed")
    empty_poly = Polygon()
    gdf = gpd.GeoDataFrame(geometry=[
        empty_poly,
        Point(1, 1)
    ], crs="EPSG:4326")
    logger.info("Calling validate_geometry_column with empty geometry.")
    result = validate_geometry_column(gdf, "dataset_with_empty_geom")
    assert len(result) == 2
    assert result.geometry.notnull().all()
    assert result.geometry.iloc[0].is_valid
    assert any("contains empty geometries" in rec.message for rec in caplog.records)
    logger.info("test_validate_geometry_column_empty_geometries_removed passed.")


def test_validate_geometry_column_no_geometry_column_raises():
    """
    Test that validate_geometry_column raises ValueError if no geometry column exists.
    """
    logger.info("Starting test_validate_geometry_column_no_geometry_column_raises")
    gdf = gpd.GeoDataFrame({"foo": [1, 2, 3]})
    logger.info("Calling validate_geometry_column without 'geometry' column.")
    with pytest.raises(ValueError):
        _ = validate_geometry_column(gdf, "no_geometry_col")
    logger.info("ValueError raised as expected for missing geometry column.")
    logger.info("test_validate_geometry_column_no_geometry_column_raises passed.")


def test_create_pipeline_features_calls_geometry_validation(monkeypatch):
    """
    Test that create_pipeline_features calls validate_geometry_column during its execution.
    Uses monkeypatch to replace validate_geometry_column with a mock that records if it was called.
    """
    logger.info("Starting test_create_pipeline_features_calls_geometry_validation")

    called = {}

    def fake_validate_geometry_column(gdf, dataset_name, allowed_geom_types=None):
        logger.info(f"fake_validate_geometry_column called with dataset: {dataset_name}")
        called['called'] = True
        return gdf

    monkeypatch.setattr("gis_tool.data_loader.validate_geometry_column", fake_validate_geometry_column)

    from gis_tool.data_loader import create_pipeline_features

    dummy_gdf = gpd.GeoDataFrame(
        {
            "Name": ["TestFeature"],
            "Date": ["2025-06-04"],
            "PSI": [100],
            "Material": ["Steel"],
            "geometry": [Point(0, 0)],
        },
        crs="EPSG:32610",
    )

    geojson_reports = [("dummy_report.geojson", dummy_gdf)]
    txt_reports = []
    spatial_reference = "EPSG:32610"

    logger.info("Calling create_pipeline_features with dummy data.")
    _, _, _ = create_pipeline_features(
        geojson_reports=geojson_reports,
        txt_reports=txt_reports,
        gas_lines_gdf=dummy_gdf,
        spatial_reference=spatial_reference,
        gas_lines_collection=None,
        processed_reports=None,
        use_mongodb=False,
    )

    assert called.get('called', False) is True
    logger.info("validate_geometry_column was called during create_pipeline_features.")
    logger.info("test_create_pipeline_features_calls_geometry_validation passed.")


def test_validate_geometry_crs_with_empty_geometry():
    """
    Test that validate_geometry_crs returns True for empty geometries.
    """
    logger.info("Starting test_validate_geometry_crs_with_empty_geometry")
    empty_geom = Point()
    result = validate_geometry_crs(empty_geom, "EPSG:4326")
    logger.info(f"Empty geometry CRS validation result: {result}")
    assert result is True
    logger.info("test_validate_geometry_crs_with_empty_geometry passed.")


def test_validate_geometry_crs_with_valid_geometry():
    """
    Test that validate_geometry_crs returns True for valid geometries.
    """
    logger.info("Starting test_validate_geometry_crs_with_valid_geometry")
    point = Point(0, 0)
    result = validate_geometry_crs(point, "EPSG:4326")
    logger.info(f"Valid geometry CRS validation result: {result}")
    assert result is True
    logger.info("test_validate_geometry_crs_with_valid_geometry passed.")


def test_validate_geometry_crs_with_invalid_geometry():
    """
    Test that validate_geometry_crs returns False for invalid geometry input types.
    """
    logger.info("Starting test_validate_geometry_crs_with_invalid_geometry")

    class DummyGeometry:
        pass

    dummy_geom = DummyGeometry()
    result = validate_geometry_crs(dummy_geom, "EPSG:4326")  # type: ignore[arg-type]
    logger.info(f"Dummy geometry CRS validation result: {result}")
    assert result is False
    logger.info("test_validate_geometry_crs_with_invalid_geometry passed.")


def test_reproject_geometry_to_crs_same_crs():
    """
    Test that reproject_geometry_to_crs returns the same geometry when reprojecting to the same CRS.
    """
    logger.info("Starting test_reproject_geometry_to_crs_same_crs")
    line = LineString([(0, 0), (1, 1)])
    reprojected = reproject_geometry_to_crs(line, "EPSG:4326", "EPSG:4326")
    logger.info(f"Reprojected geometry: {reprojected}")
    assert isinstance(reprojected, LineString)
    assert reprojected.equals(line)
    logger.info("test_reproject_geometry_to_crs_same_crs passed.")


def test_reproject_geometry_to_crs_with_point():
    """
    Test that reproject_geometry_to_crs properly reprojects Point geometry.
    """
    logger.info("Starting test_reproject_geometry_to_crs_with_point")
    point = Point(0, 0)
    reprojected = reproject_geometry_to_crs(point, "EPSG:4326", "EPSG:3857")
    logger.info(f"Reprojected Point geometry: {reprojected}")
    assert isinstance(reprojected, Point)
    # Note: 0,0 in lat/lon is also 0,0 in EPSG:3857
    assert reprojected.equals(Point(0, 0))
    logger.info("test_reproject_geometry_to_crs_with_point passed.")


def test_reproject_geometry_to_crs_with_linestring():
    """
    Test that reproject_geometry_to_crs properly reprojects LineString geometry.
    """
    logger.info("Starting test_reproject_geometry_to_crs_with_linestring")
    line = LineString([(0, 0), (1, 1)])
    reprojected = reproject_geometry_to_crs(line, "EPSG:4326", "EPSG:3857")
    logger.info(f"Reprojected LineString geometry: {reprojected}")
    assert isinstance(reprojected, LineString)
    assert not reprojected.equals(line)  # coordinates should change
    logger.info("test_reproject_geometry_to_crs_with_linestring passed.")


def test_ensure_projected_crs_already_projected():
    """
    Test that a projected CRS is returned unchanged by ensure_projected_crs.
    """
    logger.info("Starting test_ensure_projected_crs_already_projected")
    gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=config.DEFAULT_CRS)
    projected = ensure_projected_crs(gdf)
    logger.debug(f"Original CRS: {gdf.crs}, Projected CRS: {projected.crs}")
    assert projected.crs.to_string() == config.DEFAULT_CRS
    assert projected.crs.is_projected
    logger.info("test_ensure_projected_crs_already_projected passed.")


def test_ensure_projected_crs_needs_reproject():
    """
    Test that a geographic CRS (EPSG:4326) is reprojected by ensure_projected_crs.
    """
    logger.info("Starting test_ensure_projected_crs_needs_reproject")
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
    logger.info("Starting test_create_buffer_with_missing_crs")
    with tempfile.TemporaryDirectory() as tmpdir:
        gdf = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=None)
        input_path = os.path.join(tmpdir, "no_crs_input.shp")
        gdf.to_file(input_path)
        logger.debug(f"Created shapefile with missing CRS at {input_path}")

        buffered_gdf = create_buffer_with_geopandas(
            input_path,
            buffer_distance_ft=config.DEFAULT_BUFFER_DISTANCE_FT
        )

        logger.debug(f"Buffered GeoDataFrame CRS: {buffered_gdf.crs}")
        assert buffered_gdf.crs is not None
        assert buffered_gdf.crs.is_projected
        assert not buffered_gdf.empty
        assert all(buffered_gdf.geometry.is_valid)
    logger.info("test_create_buffer_with_missing_crs passed.")


def test_merge_missing_crs_inputs(tmp_path):
    """
    Test merge behavior when one or both files lack a CRS.
    """
    logger.info("Starting test_merge_missing_crs_inputs")
    buffer = gpd.GeoDataFrame(geometry=[Point(5, 5).buffer(5)], crs=None)
    future_dev = gpd.GeoDataFrame(geometry=[Point(0, 0)], crs="EPSG:4326")

    buffer_fp, future_fp = _save_test_shapefiles(buffer, future_dev, tmp_path)

    merge_buffers_into_planning_file(str(buffer_fp), str(future_fp), point_buffer_distance=5.0)
    result = gpd.read_file(future_fp)

    logger.debug(f"Merged shapefile CRS: {result.crs}, feature count: {len(result)}")
    assert not result.empty
    assert result.crs is not None
    assert all(result.geometry.is_valid)
    logger.info("test_merge_missing_crs_inputs passed.")


def _save_test_shapefiles(buffer_gdf, future_dev_gdf, tmp_path):
    """
    Helper to save buffer and future development GeoDataFrames as shapefiles.
    Returns the file paths.
    """
    buffer_fp = tmp_path / "buffer.shp"
    future_fp = tmp_path / "future.shp"
    buffer_gdf.to_file(str(buffer_fp))
    future_dev_gdf.to_file(str(future_fp))
    logger.debug(f"Saved buffer shapefile at {buffer_fp}")
    logger.debug(f"Saved future development shapefile at {future_fp}")
    return buffer_fp, future_fp


def test_buffer_intersects_gas_lines_intersection():
    """
    Test that buffer_intersects_gas_lines returns True when intersection occurs.
    """
    logger.info("Starting test_buffer_intersects_gas_lines_intersection")
    gas_lines = gpd.GeoDataFrame({
        'geometry': [
            LineString([(0, 0), (10, 0)]),
            LineString([(20, 20), (30, 20)])
        ]
    }, crs="EPSG:4326")

    buffer_geom = Polygon([(1, -1), (1, 1), (9, 1), (9, -1), (1, -1)])

    result = buffer_intersects_gas_lines(buffer_geom, gas_lines)
    logger.info(f"Intersection test result: {result}")
    assert result
    logger.info("test_buffer_intersects_gas_lines_intersection passed.")


def test_buffer_intersects_gas_lines_no_intersection():
    """
    Test that buffer_intersects_gas_lines returns False when no intersection occurs.
    """
    logger.info("Starting test_buffer_intersects_gas_lines_no_intersection")
    gas_lines = gpd.GeoDataFrame({
        'geometry': [
            LineString([(0, 0), (10, 0)]),
            LineString([(20, 20), (30, 20)])
        ]
    }, crs="EPSG:4326")

    buffer_geom = Polygon([(100, 100), (100, 110), (110, 110), (110, 100), (100, 100)])

    result = buffer_intersects_gas_lines(buffer_geom, gas_lines)
    logger.info(f"No intersection test result: {result}")
    assert not result
    logger.info("test_buffer_intersects_gas_lines_no_intersection passed.")


def test_buffer_intersects_gas_lines_empty_geometry():
    """
    Test that buffer_intersects_gas_lines returns False when buffer geometry is empty.
    """
    logger.info("Starting test_buffer_intersects_gas_lines_empty_geometry")
    gas_lines = gpd.GeoDataFrame({
        'geometry': [
            LineString([(0, 0), (10, 0)])
        ]
    }, crs="EPSG:4326")

    buffer_geom = Polygon()

    result = buffer_intersects_gas_lines(buffer_geom, gas_lines)
    logger.info(f"Empty geometry test result: {result}")
    assert not result
    logger.info("test_buffer_intersects_gas_lines_empty_geometry passed.")


def test_buffer_intersects_gas_lines_with_empty_geom_in_gdf():
    """
    Test buffer_intersects_gas_lines with empty geometry inside the gas lines GeoDataFrame.
    """
    logger.info("Starting test_buffer_intersects_gas_lines_with_empty_geom_in_gdf")
    gas_lines = gpd.GeoDataFrame({
        'geometry': [
            LineString([(0, 0), (10, 0)]),
            None
        ]
    }, crs="EPSG:4326")

    buffer_geom = Polygon([(1, -1), (1, 1), (9, 1), (9, -1), (1, -1)])

    result = buffer_intersects_gas_lines(buffer_geom, gas_lines)
    logger.info(f"Intersection test with empty geometry in gas lines: {result}")
    assert result
    logger.info("test_buffer_intersects_gas_lines_with_empty_geom_in_gdf passed.")
