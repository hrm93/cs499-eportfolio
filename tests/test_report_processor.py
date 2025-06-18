import logging
import pytest
import fiona.errors
import geopandas as gpd

from shapely.geometry import Point

from gis_tool.main import process_report_chunk

# Logger setup for test module
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs during testing


def test_process_report_chunk_error_logging(monkeypatch: pytest.MonkeyPatch, caplog, tmp_path) -> None:
    """Verify error logging when create_pipeline_features fails inside process_report_chunk."""
    logger.info("Starting test_process_report_chunk_error_logging.")

    input_folder = tmp_path / "input_folder"
    input_folder.mkdir()

    # Add a dummy .geojson file to trigger create_pipeline_features
    dummy_report = input_folder / "dummy_report.geojson"
    dummy_report.write_text('{"type": "FeatureCollection", "features": []}')

    dummy_gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, crs="EPSG:4326")

    # Monkeypatch read_file to return dummy data
    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)

    # Monkeypatch create_pipeline_features to raise FileNotFoundError
    monkeypatch.setattr(
        "gis_tool.report_processor.create_pipeline_features",
        lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError("Simulated missing file"))
    )

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder / "gas_lines.shp"),
            reports_folder=input_folder,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert any("I/O error in chunk" in record.getMessage() for record in caplog.records), (
        "Expected error log not found.\nCaptured logs:\n" +
        "\n".join(record.getMessage() for record in caplog.records)
    )

    logger.info("Completed test_process_report_chunk_error_logging.")


def test_process_report_chunk_success(monkeypatch: pytest.MonkeyPatch, caplog, tmp_path):
    """Test successful execution of process_report_chunk."""
    logger.info("Starting test_process_report_chunk_success.")

    input_folder = tmp_path / "input_folder"
    input_folder.mkdir()

    # Create dummy report and shapefile
    (input_folder / "dummy_report.geojson").write_text('{"type": "FeatureCollection", "features": []}')
    gas_lines_path = input_folder / "gas_lines.shp"
    dummy_gdf = gpd.GeoDataFrame({"geometry": [Point(1, 1)]}, crs="EPSG:4326")

    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)

    called = {"ran": False}

    def dummy_create_pipeline_features(**kwargs):
        called["ran"] = True
        logger.info("Dummy create_pipeline_features called.")

    monkeypatch.setattr("gis_tool.report_processor.create_pipeline_features", dummy_create_pipeline_features)

    with caplog.at_level(logging.INFO):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(gas_lines_path),
            reports_folder=input_folder,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert called["ran"], "Expected create_pipeline_features to be called."
    assert any("Finished processing chunk" in record.getMessage() for record in caplog.records)

    logger.info("Completed test_process_report_chunk_success.")


def test_process_report_chunk_shapefile_failure(monkeypatch: pytest.MonkeyPatch, caplog, tmp_path):
    """Test failure in reading the shapefile triggers correct error logging."""
    logger.info("Starting test_process_report_chunk_shapefile_failure.")

    input_folder = tmp_path / "input_folder"
    input_folder.mkdir()
    (input_folder / "dummy_report.geojson").write_text('{"type": "FeatureCollection", "features": []}')

    # Simulate fiona error when reading shapefile
    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: (_ for _ in ()).throw(fiona.errors.DriverError("broken shapefile")))

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder / "broken.shp"),
            reports_folder=input_folder,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert any("I/O error in chunk" in record.getMessage() for record in caplog.records)

    logger.info("Completed test_process_report_chunk_shapefile_failure.")


def test_process_report_chunk_unexpected_exception(monkeypatch: pytest.MonkeyPatch, caplog, tmp_path):
    """Test unexpected exception is caught and logged properly."""
    logger.info("Starting test_process_report_chunk_unexpected_exception.")

    input_folder = tmp_path / "input_folder"
    input_folder.mkdir()
    (input_folder / "dummy_report.geojson").write_text('{"type": "FeatureCollection", "features": []}')
    dummy_gdf = gpd.GeoDataFrame({"geometry": [Point(1, 1)]}, crs="EPSG:4326")

    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)

    # Raise ValueError to simulate unexpected failure
    monkeypatch.setattr(
        "gis_tool.report_processor.create_pipeline_features",
        lambda *a, **k: (_ for _ in ()).throw(ValueError("unexpected failure"))
    )

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder / "gas_lines.shp"),
            reports_folder=input_folder,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert any("Unexpected error in chunk" in record.getMessage() for record in caplog.records)

    logger.info("Completed test_process_report_chunk_unexpected_exception.")


def test_process_report_chunk_mongodb_warning(monkeypatch: pytest.MonkeyPatch, caplog, tmp_path):
    """Check that MongoDB insert warning is logged in worker context."""
    logger.info("Starting test_process_report_chunk_mongodb_warning.")

    input_folder = tmp_path / "input_folder"
    input_folder.mkdir()
    (input_folder / "dummy_report.geojson").write_text('{"type": "FeatureCollection", "features": []}')
    dummy_gdf = gpd.GeoDataFrame({"geometry": [Point(1, 1)]}, crs="EPSG:4326")

    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)
    monkeypatch.setattr("gis_tool.report_processor.create_pipeline_features", lambda **kwargs: None)

    with caplog.at_level(logging.INFO):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder / "gas_lines.shp"),
            reports_folder=input_folder,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=True,
        )

    assert any("MongoDB insert disabled for this process" in record.getMessage() for record in caplog.records)

    logger.info("Completed test_process_report_chunk_mongodb_warning.")
